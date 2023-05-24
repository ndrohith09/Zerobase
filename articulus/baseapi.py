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

#*main
def main():
    cursor.execute('CREATE TABLE IF NOT EXISTS Birds (id SERIAL PRIMARY KEY, name VARCHAR(255), breed VARCHAR(255))') 
    conn.commit() 
    print("Table created successfully")

#*Dataclasses
@strawberry.type
class Birds:
    id: str
    name: str
    breed: str

 

@strawberry.type
class Query:
    #*graphquery
    @strawberry.field
    def all_birds(self) -> typing.List[Birds]:
        cursor.execute("SELECT * FROM Birds")
        lst = cursor.fetchall()
        birds = []
        for i in lst:
            birds.append(Birds(id=i[0], name=i[1], breed=i[2]))
        return birds

    @strawberry.field
    def get_birds(self, id: str) -> Birds:
        cursor.execute("SELECT * FROM Birds WHERE id = %s", (id,))
        lst = cursor.fetchone()
        return Birds(id=lst[0], name=lst[1], breed=lst[2])
         
    

@strawberry.type
class Mutation:
    #*graphmutation
    @strawberry.mutation
    def create_birds(self, name: str, breed: str) -> Birds:
        cursor.execute("INSERT INTO Birds (name, breed) VALUES (%s, %s) RETURNING id", (name, breed))
        conn.commit()
        birds_id = cursor.fetchone()[0]
        return Birds(id=birds_id,name=name, breed=breed)
    
    @strawberry.mutation
    def update_birds(self, id: str, name: str, breed: str) -> Birds:
        
        cursor.execute("UPDATE Birds SET name=%s, breed=%s WHERE id = %s", (name, breed, id))
        conn.commit()
        return Birds(id=id,name=name, breed=breed)
    
    @strawberry.mutation
    def delete_birds(self, id: str) -> Birds:
        cursor.execute("SELECT * FROM Birds WHERE id = %s", (id,))
        lst = cursor.fetchone()
        if lst is None:
            return Birds(name='No Data Found', breed='No Data Found')

        cursor.execute("DELETE FROM Birds WHERE id = %s", (id,))
        conn.commit()
        return Birds(id=id,id=lst[0], name=lst[1], breed=lst[2])
     

schema = strawberry.Schema(Query , Mutation)


graphql_app = GraphQLRouter(
    schema,
)


app = FastAPI()
app.include_router(graphql_app, prefix="/graphql")

if __name__ == "__main__": 
    try:
        main()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error) 
    uvicorn.run(app, host='0.0.0.0', port=8000)
