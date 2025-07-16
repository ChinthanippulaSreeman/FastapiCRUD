import uuid
from typing import List, Optional

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field, validator

# --- Configuration & Boto3 Setup ---

# For a real application, consider using a settings management library
# to load these from environment variables or a .env file.
DYNAMODB_ENDPOINT_URL = "http://localhost:8000"
DYNAMODB_REGION = "us-west-2"
AWS_ACCESS_KEY_ID = "dummy"
AWS_SECRET_ACCESS_KEY = "dummy"
TABLE_NAME = "BFS"  # Changed to match Crudtable.py

# Initialize FastAPI app
app = FastAPI(
    title="DynamoDB CRUD API",
    description="A simple FastAPI application for CRUD operations with DynamoDB.",
    version="1.0.0",
)

# This is a global variable that will hold the table resource.
table = None

# --- Pydantic Models ---
# These models define the structure of your data and are used for validation,
# serialization, and documentation.

class ItemBase(BaseModel):
    name: str = Field(..., example="My Item")
    description: Optional[str] = Field(None, example="A description of my item.")

    @validator("name", "description", pre=True, always=True)
    def check_is_str(cls, v):
        """Ensures that the value is a string if it's not None."""
        if v is not None and not isinstance(v, str):
            raise ValueError(
                "please check the input values please enter string values only"
            )
        return v

class ItemCreate(ItemBase):
    pass

# Updated to match BFS table schema
class Item(BaseModel):
    ID: str = Field(..., example="123")
    CITY: str = Field(..., example="SomeCity")
    state: str = Field(..., example="SomeState")
    name: str = Field(..., example="Item Name")

    @validator("*", pre=True)
    def check_all_are_str(cls, v):
        """Ensures all fields are strings."""
        if not isinstance(v, str):
            raise ValueError(
                "please check the input values please enter string values only"
            )
        return v
# --- FastAPI Events ---

@app.on_event("startup")
def startup_event():
    """
    Note: Since this reuses logic from Crudtable.py, we're connecting to the 'BFS' table 
    with the composite key (ID, CITY).  Adjust models and endpoints accordingly.
    On application startup, connect to DynamoDB.
    """
    global table
    print("Connecting to DynamoDB...")
    try:
        dynamodb_resource = boto3.resource(
            'dynamodb',
            endpoint_url=DYNAMODB_ENDPOINT_URL,
            region_name=DYNAMODB_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        table = dynamodb_resource.Table(TABLE_NAME)
        # A quick check to see if the table exists. This will raise an error if not found.
        table.load() 
        print(f"Successfully connected to DynamoDB table: '{TABLE_NAME}'")
    except ClientError as e:
        # This is a critical error if the table doesn't exist.
        print(f"Error connecting to DynamoDB table '{TABLE_NAME}': {e.response['Error']['Message']}")
        print("Please ensure the table exists. You can run 'createtable.py'.")
        table = None # Set table to None so endpoints can handle it

# --- FastAPI Endpoints ---

def get_table():
    """Dependency to check for DB connection"""
    if not table:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Database connection not available")
    return table

@app.post("/items/", response_model=Item, status_code=status.HTTP_201_CREATED, tags=["Items"])
def create_item(item: Item):
    """
    Create a new item in the database.
    - A  ID  and city is needed for primary key to be  generated.
    """
    db_table = get_table()
    db_table.put_item(Item=item.dict())
    return item




@app.get("/items/{id}/{city}", response_model=Item, tags=["Items"])
def read_item(id: str, city: str):
    """
    Retrieve a single item by its ID and City (composite key).
    """
    db_table = get_table()
    response = db_table.get_item(Key={'ID': id, 'CITY': city})
    
    item = response.get('Item')
    if not item:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")
    
    return Item(**item)  # Use Pydantic model for consistent output


@app.get("/items/", response_model=List[Item], tags=["Items"])
def read_all_items():
    """
    Retrieve all items from the database (similar to Crudtable.py's retrieve_all).
    Note: scan is inefficient for large tables; consider pagination for production use.
    """
    db_table = get_table()
    response = db_table.scan()
    items = response.get('Items', [])
    return [Item(**item) for item in items] # Convert to Pydantic models




@app.delete("/items/{id}/{city}", status_code=status.HTTP_200_OK, tags=["Items"])
def delete_item(id: str, city: str):
    """
    Delete an item by its ID and City (composite key).
    """
    db_table = get_table()
    try:
        db_table.delete_item(
            Key={'ID': id, 'CITY': city},
            ConditionExpression='attribute_exists(ID) AND attribute_exists(CITY)'
        )
        return {
            "ok": True,
            "message": f"Item with ID '{id}' and City '{city}' deleted successfully."
        }
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Item with ID '{id}' and City '{city}' not found.")
        else:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {e}")

# To run this application:
# 1. Make sure you have FastAPI and uvicorn installed:
#    pip install fastapi "uvicorn[standard]" boto3
# 2. Make sure your local DynamoDB is running.
# 3. Run the `createtable.py` script once to create the 'CRUD' table.
# 4. Save this code as `main.py` and run from your terminal:
#    uvicorn main:app --reload
# 5. Access the interactive API docs at http://127.0.0.1:8000/docs