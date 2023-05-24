import strawberry
import uvicorn
from fastapi import FastAPI, Depends, Request, WebSocket, BackgroundTasks
from strawberry.types import Info
from strawberry.fastapi import GraphQLRouter
import typing
import json 
import psycopg2
import os

try:

    # Establish a connection
    conn = psycopg2.connect(
        host=os.environ.get('PG_HOST'),
        port= os.environ.get('PG_PORT'),
        database=os.environ.get('PG_DATABASE'),
        user=os.environ.get('PG_USER'),
        password=os.environ.get('PG_PASSWORD')
    )
    cursor = conn.cursor()
    print("Connected to the PostgreSQL database")

except (Exception, psycopg2.DatabaseError) as error:
    print("Cannot connect to the PostgreSQL database")
    print(error)  

    
def main():
    
    print("Inside main")

#*Dataclasses
    
@strawberry.type
class Query:
    #*graphquery
        
@strawberry.type
class Mutation:
    #*graphmutation

schema = strawberry.Schema(Query, Mutation)


graphql_app = GraphQLRouter(
    schema,
)


app = FastAPI()
app.include_router(graphql_app, prefix="/graphql")

# main function 
if __name__ == "__main__": 
    print(os.environ['PG_HOST'] , os.environ['PG_DATABASE'] , os.environ['PG_USER'] , os.environ['PG_PASSWORD']) 
    uvicorn.run(app, host='0.0.0.0', port=8000)
