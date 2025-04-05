import os
import logging
import pyodbc
import re
import requests
import ollama
import time
import threading
import pandas as pd
import json
from typing import List, Dict, Any, Generator, Optional, Tuple
from contextlib import contextmanager
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext
from sentence_transformers import SentenceTransformer

# Suppress TensorFlow warnings
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("northwind_bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration constants
MAX_SQL_ATTEMPTS = 3
MAX_RESPONSE_TOKENS = 1024
MAX_SQL_TOKENS = 512
MAX_REQUESTS_PER_MINUTE = 10
MAX_RESULTS = 50
DB_CONNECTION_TIMEOUT = 30

class DatabaseManager:
    """Handles all database operations with connection pooling and retries"""
    
    @staticmethod
    @contextmanager
    def get_connection() -> Generator[pyodbc.Connection, None, None]:
        """Database connection context manager with retries and Windows Authentication"""
        conn = None
        attempt = 0
        last_exception = None
        
        while attempt < MAX_SQL_ATTEMPTS:
            try:
                db_server = os.getenv("DB_SERVER")
                db_name = os.getenv("DB_NAME")
                
                if not db_server or not db_name:
                    raise ValueError("DB_SERVER and DB_NAME environment variables must be set")
                
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={db_server};"
                    f"DATABASE={db_name};"
                    "Trusted_Connection=yes;"
                    f"Connection Timeout={DB_CONNECTION_TIMEOUT};"
                )
                
                conn = pyodbc.connect(conn_str, timeout=DB_CONNECTION_TIMEOUT)
                conn.autocommit = False
                logger.info(f"Database connection established (attempt {attempt + 1})")
                
                try:
                    yield conn
                    conn.commit()
                    return
                except Exception as e:
                    if conn:
                        conn.rollback()
                    raise
                finally:
                    if conn:
                        conn.close()
                    
            except pyodbc.Error as e:
                last_exception = e
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
                attempt += 1
                if attempt < MAX_SQL_ATTEMPTS:
                    time.sleep(1 + attempt)
        
        logger.error("All database connection attempts failed")
        raise ConnectionError("Failed to connect to database") from last_exception

    @staticmethod
    def execute_query(query: str) -> Tuple[List[Dict], Optional[str]]:
        """Execute a SQL query and return results or error message"""
        try:
            logger.info(f"Executing query: {query}")
            
            if not query.strip().lower().startswith('select'):
                logger.warning("Non-SELECT query attempted")
                return [], "Only SELECT queries are allowed"
                
            if not DatabaseManager._is_query_safe(query):
                logger.warning("Unsafe query detected")
                return [], "Query contains potentially unsafe operations"
                
            with DatabaseManager.get_connection() as conn:
                with conn.cursor() as cursor:
                    try:
                        cursor.execute(query)
                        
                        if not cursor.description:
                            return [], "No results returned"
                            
                        columns = [col[0] for col in cursor.description]
                        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                        logger.info(f"Query returned {len(results)} rows")
                        return results, None
                    except pyodbc.Error as e:
                        error_msg = str(e).split('\n')[0]
                        logger.error(f"SQL execution error: {error_msg}")
                        return [], f"SQL error: {error_msg}"
                    
        except Exception as e:
            logger.error(f"Unexpected database error: {e}", exc_info=True)
            return [], "Unexpected database error"

    @staticmethod
    def _is_query_safe(query: str) -> bool:
        """Check if the query is safe to execute"""
        dangerous_patterns = [
            r'\b(drop|delete|truncate|alter|shutdown|insert|update|merge)\b',
            r';.*\b(exec|execute|xp_cmdshell|sp_)\b',
            r'\bunion\b.*\bselect\b',
            r'\bselect\b.*\bfrom\b.*\bwhere\b.*\b1\s*=\s*1\b',
            r'/\*.*\*/',
            r'--.*$',
            r'[\/\\]'
        ]
        query_lower = query.lower()
        return not any(re.search(pattern, query_lower) for pattern in dangerous_patterns)

    @staticmethod
    def get_schema_info(force_refresh=False) -> Dict[str, Any]:
        """Dynamically fetch and cache schema information"""
        cache_file = "schema_cache.json"
        
        # Return cached schema if available and not forcing refresh
        if not force_refresh and os.path.exists(cache_file):
            with open(cache_file, 'r') as f:
                return json.load(f)
        
        # Query to get schema information (optimized version)
        schema_query = """
        SELECT 
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.IS_NULLABLE
        FROM INFORMATION_SCHEMA.TABLES t
        JOIN INFORMATION_SCHEMA.COLUMNS c 
            ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION;
        """
        
        # Get foreign key relationships separately
        fk_query = """
        SELECT
            KCU1.TABLE_NAME AS FK_TABLE,
            KCU1.COLUMN_NAME AS FK_COLUMN,
            KCU2.TABLE_NAME AS REFERENCED_TABLE,
            KCU2.COLUMN_NAME AS REFERENCED_COLUMN
        FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1
            ON KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2
            ON KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME
        """
        
        with DatabaseManager.get_connection() as conn:
            with conn.cursor() as cursor:
                # Get tables and columns
                cursor.execute(schema_query)
                schema_data = [dict(zip([col[0] for col in cursor.description], row)) 
                             for row in cursor.fetchall()]
                
                # Get foreign keys
                cursor.execute(fk_query)
                fk_data = [dict(zip([col[0] for col in cursor.description], row)) 
                          for row in cursor.fetchall()]
        
        # Organize the schema information
        schema_info = {
            "tables": {},
            "relationships": []
        }
        
        # Process tables and columns
        for row in schema_data:
            table_key = f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}"
            if table_key not in schema_info["tables"]:
                schema_info["tables"][table_key] = {
                    "columns": [],
                    "primary_keys": []
                }
            
            schema_info["tables"][table_key]["columns"].append({
                "name": row["COLUMN_NAME"],
                "type": row["DATA_TYPE"],
                "nullable": row["IS_NULLABLE"] == "YES"
            })
        
        # Process foreign key relationships
        for row in fk_data:
            schema_info["relationships"].append({
                "from_table": row["FK_TABLE"],
                "from_column": row["FK_COLUMN"],
                "to_table": row["REFERENCED_TABLE"],
                "to_column": row["REFERENCED_COLUMN"]
            })
        
        # Cache the schema
        with open(cache_file, 'w') as f:
            json.dump(schema_info, f, indent=2)
        
        return schema_info

class OllamaLLM:
    """Handles all LLM-related operations including SQL generation and response formatting"""
    
    def __init__(self):
        self.primary_model = "llama3"
        self.fallback_model = "mistral"
        self.sentence_model = SentenceTransformer('all-MiniLM-L6-v2')
        logger.info(f"Ollama model initialized - Primary: {self.primary_model}, Fallback: {self.fallback_model}")
    
    def generate_sql(self, prompt: str) -> Optional[str]:
        """Generate and validate SQL - Only SELECT queries allowed"""
        for attempt in range(MAX_SQL_ATTEMPTS):
            try:
                # Get current schema information
                schema_info = DatabaseManager.get_schema_info()
                
                # Generate schema description for prompt
                schema_description = "Database Schema Information:\n\n"
                for table_key, table_info in schema_info["tables"].items():
                    schema_description += f"Table: {table_key}\n"
                    schema_description += "Columns:\n"
                    for col in table_info["columns"]:
                        schema_description += f"- {col['name']} ({col['type']}, {'nullable' if col['nullable'] else 'not null'})\n"
                    schema_description += "\n"
                
                # Add relationships
                schema_description += "Relationships:\n"
                for rel in schema_info["relationships"]:
                    schema_description += f"- {rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']}\n"
                
                response = ollama.generate(
                    model=self.primary_model,
                    prompt=f"""
                    [INST] <<SYS>>
                    You are a SQL expert for the Northwind database. Follow these rules ABSOLUTELY:
                    
                    CURRENT SCHEMA INFORMATION:
                    {schema_description}
                    
                    EXAMPLE QUERIES:
                    - Customers: "SELECT TOP {MAX_RESULTS} * FROM Customers WHERE Country = 'Germany'"
                    - Products: "SELECT TOP {MAX_RESULTS} ProductName, UnitPrice FROM Products WHERE CategoryID = 1"
                    - Orders with Shippers: "SELECT TOP {MAX_RESULTS} o.OrderID, c.CompanyName, o.ShipCity 
                                          FROM Orders o 
                                          JOIN Shippers s ON o.ShipVia = s.ShipperID 
                                          JOIN Customers c ON o.CustomerID = c.CustomerID 
                                          WHERE s.CompanyName = 'Federal Shipping'"
                    
                    STRICT REQUIREMENTS:
                    1. Only SELECT queries
                    2. Output ONLY the SQL query
                    3. Use exact column/table names from schema
                    4. SQL Server syntax (TOP {MAX_RESULTS})
                    5. Never join Customers directly to Employees
                    6. For shipper queries, join Orders.ShipVia to Shippers.ShipperID
                    7. Always include TOP {MAX_RESULTS} in your queries
                    
                    TASK: Convert this to SQL:
                    {prompt}
                    [/INST]
                    """,
                    options={
                        "temperature": 0.1,
                        "num_predict": MAX_SQL_TOKENS,
                        "stop": ["```", "[/INST]", "<<SYS>>"]
                    }
                )
                sql = self._clean_sql(response['response'])
                logger.info(f"Generated SQL: {sql}")
                
                # Validation checks
                if not sql.lower().strip().startswith('select'):
                    raise ValueError("Generated SQL must start with SELECT")
                
                if any(c in sql.lower() for c in ['/*', '--', '/']):
                    raise ValueError("SQL contains prohibited characters")
                    
                if "top" not in sql.lower() and "limit" in sql.lower():
                    raise ValueError("Use TOP instead of LIMIT")
                    
                if not DatabaseManager._is_query_safe(sql):
                    raise ValueError("Query contains unsafe operations")
                    
                if not self._validate_sql(sql):
                    raise ValueError("Invalid table relationships in query")
                    
                # Ensure TOP clause is present
                if f"top {MAX_RESULTS}" not in sql.lower():
                    sql = re.sub(r'select\s+', f'select top {MAX_RESULTS} ', sql, flags=re.IGNORECASE)
                    
                return sql
                
            except Exception as e:
                logger.warning(f"SQL generation attempt {attempt + 1} failed: {e}")
                if attempt == MAX_SQL_ATTEMPTS - 1:
                    return None
                time.sleep(1 + attempt)

    @staticmethod
    def _validate_sql(sql: str) -> bool:
        """Additional validation for SQL queries"""
        sql_lower = sql.lower()
        
        # Check for invalid joins between Customers and Employees
        if re.search(r'join\s+customers\s+.*=\s*employees\.employeeid', sql_lower) or \
           re.search(r'join\s+employees\s+.*=\s*customers\.customerid', sql_lower):
            return False
            
        # Check for other invalid relationships
        invalid_joins = [
            r'join\s+products\s+.*=\s*employees\.employeeid',
            r'join\s+orders\s+.*=\s*suppliers\.supplierid',
            r'join\s+orders\s+.*=\s*shippers\.shipperid\s+.*!=\s*orders\.shipvia'
        ]
        
        if any(re.search(pattern, sql_lower) for pattern in invalid_joins):
            return False
            
        return True

    def format_response(self, user_query: str, results: List[Dict]) -> str:
        """Format results with data sanitization and structured analysis"""
        if not results:
            return "🔍 No data found matching your query"
        
        # Convert to DataFrame for analysis
        df = pd.DataFrame(results)
        
        # Detect query type and format accordingly
        if 'ProductName' in df.columns and 'UnitPrice' in df.columns:
            return self._format_product_response(df, user_query)
        elif 'OrderID' in df.columns and ('CustomerID' in df.columns or 'ShipVia' in df.columns):
            return self._format_order_response(df, user_query)
        elif 'EmployeeID' in df.columns and 'LastName' in df.columns:
            return self._format_employee_response(df, user_query)
        elif 'CompanyName' in df.columns and 'ContactName' in df.columns:
            return self._format_company_response(df, user_query)
        elif 'ShipperID' in df.columns or 'ShipVia' in df.columns:
            return self._format_shipper_response(df, user_query)
        else:
            return self._format_general_response(df, user_query)

    def _format_product_response(self, df: pd.DataFrame, query: str) -> str:
        """Format product-related queries"""
        analysis = [
            f"📦 *Product Analysis: {query}*",
            f"📊 Total Records: {len(df)}",
            f"💰 Price Range: ${df['UnitPrice'].min():.2f}-${df['UnitPrice'].max():.2f}",
            f"🏷️ Unique Products: {df['ProductName'].nunique()}"
        ]
        
        if 'Quantity' in df.columns:
            analysis.extend([
                f"🧮 Total Units: {df['Quantity'].sum()}",
                f"📈 Avg Order Size: {df['Quantity'].mean():.1f} units"
            ])
        
        if 'CategoryID' in df.columns:
            analysis.append(f"🗂️ Categories: {df['CategoryID'].nunique()}")
            
        return "\n".join(analysis) + "\n\n" + self._format_all_records(df)

    def _format_order_response(self, df: pd.DataFrame, query: str) -> str:
        """Format order-related queries"""
        if 'OrderDate' in df.columns:
            df['OrderDate'] = pd.to_datetime(df['OrderDate'])
            date_range = f"{df['OrderDate'].min().strftime('%Y-%m-%d')} to {df['OrderDate'].max().strftime('%Y-%m-%d')}"
        else:
            date_range = "Date information not available"
        
        analysis = [
            f"📝 *Order Analysis: {query}*",
            f"📅 Date Range: {date_range}",
            f"👥 Unique Customers: {df['CustomerID'].nunique() if 'CustomerID' in df.columns else 'N/A'}",
            f"📦 Avg Items/Order: {df.groupby('OrderID').size().mean():.1f if 'OrderID' in df.columns else 'N/A'}"
        ]
        
        if 'Freight' in df.columns:
            analysis.append(f"🚚 Avg Freight Cost: ${df['Freight'].mean():.2f}")
            
        if 'ShipVia' in df.columns:
            analysis.append(f"🚛 Shipping Methods: {df['ShipVia'].nunique()}")
            
        return "\n".join(analysis) + "\n\n" + self._format_all_records(df)

    def _format_shipper_response(self, df: pd.DataFrame, query: str) -> str:
        """Format shipper-related queries"""
        analysis = [
            f"🚚 *Shipping Analysis: {query}*",
            f"📦 Total Shipments: {len(df)}",
            f"🏢 Shipping Companies: {df['CompanyName'].nunique() if 'CompanyName' in df.columns else 'N/A'}"
        ]
        
        if 'Freight' in df.columns:
            analysis.extend([
                f"💰 Total Freight Costs: ${df['Freight'].sum():.2f}",
                f"📊 Avg Freight Cost: ${df['Freight'].mean():.2f}"
            ])
            
        return "\n".join(analysis) + "\n\n" + self._format_all_records(df)

    def _format_employee_response(self, df: pd.DataFrame, query: str) -> str:
        """Format employee-related queries"""
        analysis = [
            f"👔 *Employee Analysis: {query}*",
            f"👥 Total Employees: {len(df)}",
            f"📅 Hire Date Range: {pd.to_datetime(df['HireDate']).min().strftime('%Y-%m-%d') if 'HireDate' in df.columns else 'N/A'} to {pd.to_datetime(df['HireDate']).max().strftime('%Y-%m-%d') if 'HireDate' in df.columns else 'N/A'}",
            f"🏢 Titles: {df['Title'].nunique() if 'Title' in df.columns else 'N/A'} unique"
        ]
        
        if 'TerritoryID' in df.columns:
            analysis.append(f"🗺️ Territories Covered: {df['TerritoryID'].nunique()}")
            
        return "\n".join(analysis) + "\n\n" + self._format_all_records(df)

    def _format_company_response(self, df: pd.DataFrame, query: str) -> str:
        """Format customer/supplier-related queries"""
        entity_type = "Supplier" if 'SupplierID' in df.columns else "Customer"
        
        analysis = [
            f"🏢 *{entity_type} Analysis: {query}*",
            f"🌍 Countries: {df['Country'].nunique() if 'Country' in df.columns else 'N/A'}",
            f"📞 Contacts: {df['ContactName'].nunique() if 'ContactName' in df.columns else 'N/A'}",
            f"📍 Cities: {df['City'].nunique() if 'City' in df.columns else 'N/A'}"
        ]
        
        if 'Phone' in df.columns:
            analysis.append("☎️ Phone: [REDACTED]")
            
        return "\n".join(analysis) + "\n\n" + self._format_all_records(df)

    def _format_general_response(self, df: pd.DataFrame, query: str) -> str:
        """Fallback format for unrecognized queries"""
        return (
            f"🔍 *Query Results: {query}*\n"
            f"📊 Records Found: {len(df)}\n\n"
            + self._format_all_records(df)
        )

    def _format_all_records(self, df: pd.DataFrame) -> str:
        """Generate all record display"""
        records = []
        for idx, (_, row) in enumerate(df.iterrows(), 1):
            record = [f"📌 Record {idx}:"]
            for k, v in row.items():
                if pd.notna(v):
                    record.append(f"• {k}: {self._sanitize_data(str(v))}")
            records.append("\n".join(record))
        return "\n\n".join(records)

    @staticmethod
    def _sanitize_data(value: str) -> str:
        """Remove sensitive data from results"""
        value = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL]', value)
        value = re.sub(r'\b\d{10,}\b', '[PHONE]', value)
        if any(word in value.lower() for word in ['password', 'secret', 'address', 'phone', 'fax']):
            return '[REDACTED]'
        return value
    
    @staticmethod
    def _clean_sql(sql: str) -> str:
        """Clean SQL output more thoroughly"""
        # Remove code block markers and everything after them
        sql = re.sub(r'```.*$', '', sql, flags=re.DOTALL | re.IGNORECASE)
        sql = re.sub(r'\[/INST\].*$', '', sql, flags=re.DOTALL | re.IGNORECASE)
        sql = re.sub(r'<<SYS>>.*$', '', sql, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove any remaining comments
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        
        # Remove problematic characters and extra whitespace
        sql = re.sub(r'[\/\\]', '', sql)
        sql = re.sub(r'\s+', ' ', sql).strip()
        
        # Ensure proper SELECT TOP syntax
        if "select" in sql.lower() and "top " not in sql.lower():
            sql = re.sub(r'select\s+', f'select top {MAX_RESULTS} ', sql, flags=re.IGNORECASE)
            
        # Remove any trailing semicolons and everything after them
        sql = sql.split(';')[0].strip()
        
        # Final validation - only keep the first complete SQL statement
        sql_lines = []
        for line in sql.split('\n'):
            line = line.strip()
            if line:
                sql_lines.append(line)
                if line.endswith(';'):
                    break
        sql = ' '.join(sql_lines).strip()
        
        return sql

class RateLimiter:
    """Handles rate limiting for users"""
    
    def __init__(self):
        self.user_requests = {}
        self.lock = threading.Lock()
        
    async def check_rate_limit(self, update: Update) -> bool:
        """Enforce rate limiting per user"""
        user_id = update.effective_user.id
        now = time.time()
        
        with self.lock:
            if user_id not in self.user_requests:
                self.user_requests[user_id] = []
                logger.info(f"New user detected: {user_id}")
            
            self.user_requests[user_id] = [
                t for t in self.user_requests[user_id] 
                if now - t < 60
            ]
            
            if len(self.user_requests[user_id]) >= MAX_REQUESTS_PER_MINUTE:
                logger.warning(f"Rate limit exceeded for user {user_id}")
                await update.message.reply_text("⚠️ Too many requests. Please wait a minute.")
                return False
            
            self.user_requests[user_id].append(now)
            return True

def is_greeting(text: str) -> bool:
    """Check if the text is a greeting"""
    greetings = ["hi", "hello", "hey", "good morning", "good evening", "good afternoon"]
    return any(re.search(rf"\b{greeting}\b", text, re.IGNORECASE) for greeting in greetings)

async def send_long_message(update: Update, text: str):
    """Send long messages split into chunks"""
    for i in range(0, len(text), 4096):
        await update.message.reply_text(text[i:i+4096])

async def start(update: Update, context: CallbackContext):
    """Handle /start command"""
    logger.info(f"New user started conversation: {update.effective_user.id}")
    await update.message.reply_text(
        "👋 Welcome to Northwind Database Assistant!\n\n"
        "I can help you query Northwind sales data. Try asking:\n"
        "• 'Show customers from Germany'\n"
        "• 'List products in category Beverages'\n"
        "• 'What are the recent orders?'\n"
        "• 'Show orders shipped via Federal Shipping'\n\n"
        f"I'll return up to {MAX_RESULTS} results at a time."
    )

async def handle_message(update: Update, context: CallbackContext):
    """Handle incoming messages"""
    user_id = update.effective_user.id
    user_message = update.message.text.strip()
    logger.info(f"Received message from {user_id}: {user_message}")
    
    if not await rate_limiter.check_rate_limit(update):
        return
    
    try:
        if is_greeting(user_message):
            await update.message.reply_text("👋 Hello! How can I help with Northwind data today?")
            return
            
        query = ollama_llm.generate_sql(user_message)
        
        if not query:
            logger.warning("Failed to generate SQL query")
            await update.message.reply_text("🤔 I couldn't generate a valid query. Try something like 'Show customers from Germany'")
            return
            
        results, error = DatabaseManager.execute_query(query)
        
        if error:
            logger.error(f"Query execution failed: {error}")
            if "invalid column name" in error.lower():
                await update.message.reply_text("⚠️ There was an issue with the query structure. Please try rephrasing your request.")
            else:
                await update.message.reply_text(f"⚠️ {error}")
            return
            
        if not results:
            logger.info("Query returned no results")
            await update.message.reply_text(f"🔍 No data found for: {user_message}")
        else:
            response = ollama_llm.format_response(user_message, results)
            logger.info("Sending formatted response to user")
            await send_long_message(update, response)
            
    except Exception as e:
        logger.error(f"Error handling message: {e}", exc_info=True)
        await update.message.reply_text("⚠️ An unexpected error occurred. Please try again.")

def verify_dependencies() -> bool:
    """Verify all required dependencies are available"""
    try:
        # Check Ollama
        try:
            response = requests.get('http://localhost:11434', timeout=5)
            if response.status_code != 200:
                raise ConnectionError("Ollama not responding")
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Ollama connection failed: {e}")
        
        # Check database connection
        def test_db_connection():
            try:
                with DatabaseManager.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        return cursor.fetchone()[0] == 1
            except Exception as e:
                logger.error(f"Database test failed: {e}")
                return False
        
        if not test_db_connection():
            raise ConnectionError("Database test query failed")
            
        logger.info("All dependencies verified successfully")
        return True
        
    except Exception as e:
        logger.critical(f"Startup check failed: {e}")
        return False

def main():
    """Main application entry point"""
    try:
        logger.info("Starting Northwind Database Bot")
        
        if not verify_dependencies():
            raise RuntimeError("Dependency verification failed")
        
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        if not token:
            raise ValueError("TELEGRAM_BOT_TOKEN environment variable must be set")
        
        app = Application.builder() \
            .token(token) \
            .read_timeout(30) \
            .write_timeout(30) \
            .build()
            
        app.add_handler(CommandHandler("start", start))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        
        logger.info("Bot is ready and polling")
        app.run_polling()
        
    except Exception as e:
        logger.critical(f"Application failed: {e}")
        exit(1)

# Initialize components
ollama_llm = OllamaLLM()
rate_limiter = RateLimiter()

if __name__ == "__main__":
    main()
