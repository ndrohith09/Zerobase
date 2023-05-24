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
    cursor.execute('CREATE TABLE IF NOT EXISTS Animals (id SERIAL PRIMARY KEY, name VARCHAR(255), country VARCHAR(255))') 
    conn.commit() 
    print("Table created successfully")

#*Dataclasses
@strawberry.type
class Animals:
    id: str
    name: str
    country: str

 

@strawberry.type
class Query:
    #*graphquery
    @strawberry.field
    def all_animals(self) -> typing.List[Animals]:
        cursor.execute("SELECT * FROM Animals")
        lst = cursor.fetchall()
        animals = []
        for i in lst:
            animals.append(Animals(id=i[0], name=i[1], country=i[2]))
        return animals

    @strawberry.field
    def get_animals(self, id: str) -> Animals:
        cursor.execute("SELECT * FROM Animals WHERE id = %s", (id,))
        lst = cursor.fetchone()
        return Animals(id=lst[0], name=lst[1], country=lst[2])
         
    

@strawberry.type
class Mutation:
    #*graphmutation
    @strawberry.mutation
    def create_animals(self, name: str, country: str) -> Animals:
        cursor.execute("INSERT INTO Animals (name, country) VALUES (%s, %s) RETURNING id", (name, country))
        conn.commit()
        animals_id = cursor.fetchone()[0]
        return Animals(id=animals,name=name, country=country)
    
    @strawberry.mutation
    def update_animals(self, id: str, name: str, country: str) -> Animals:
        
        cursor.execute("UPDATE Animals SET name=%s, country=%s WHERE id = %s", (name, country, id))
        conn.commit()
        return Animals(id=id,name=name, country=country)
    
    @strawberry.mutation
    def delete_animals(self, id: str) -> Animals:
        cursor.execute("SELECT * FROM Animals WHERE id = %s", (id,))
        lst = cursor.fetchone()
        if lst is None:
            return Animals(name='No Data Found', country='No Data Found')

        cursor.execute("DELETE FROM Animals WHERE id = %s", (id,))
        conn.commit()
        return Animals(id=id,id=lst[0], name=lst[1], country=lst[2])
     

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
