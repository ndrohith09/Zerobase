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
    cursor.execute(CREATE TABLE IF NOT EXISTS Animals (id SERIAL PRIMARY KEY, name VARCHAR(255), wild VARCHAR(255))) 
    conn.commit()
    cursor.execute(CREATE TABLE IF NOT EXISTS Birds (id SERIAL PRIMARY KEY, name VARCHAR(255), family VARCHAR(255))) 
    conn.commit()

    
    print("Inside main")

#*Dataclasses
@strawberry.type
class Animals:
    id: str
    name: str
    wild: str

@strawberry.type
class Birds:
    id: str
    name: str
    family: str

    
@strawberry.type
class Query:
    
    
    #*graphquery
    @strawberry.field
    def all_animals(self) -> typing.List[Animals]:
        cursor.execute("SELECT * FROM Animals")
        lst = cursor.fetchall()
        animals = []
        for i in lst:
            animals.append(Animals(id=i[0], name=i[1], wild=i[2]))
        return animals

    @strawberry.field
    def get_animals(self, id: str) -> Animals:
        cursor.execute("SELECT * FROM Animals WHERE id = %s", (id,))
        lst = cursor.fetchone()
        return Animals(id=lst[0], name=lst[1], wild=lst[2])
    
    @strawberry.field
    def all_birds(self) -> typing.List[Birds]:
        cursor.execute("SELECT * FROM Birds")
        lst = cursor.fetchall()
        birds = []
        for i in lst:
            birds.append(Birds(id=i[0], name=i[1], family=i[2]))
        return birds

    @strawberry.field
    def get_birds(self, id: str) -> Birds:
        cursor.execute("SELECT * FROM Birds WHERE id = %s", (id,))
        lst = cursor.fetchone()
        return Birds(id=lst[0], name=lst[1], family=lst[2])
    
        
@strawberry.type
class Mutation:
    
    
    #*graphmutation
    @strawberry.mutation
    def create_animals(self, name: str, wild: str) -> Animals:
        cursor.execute("INSERT INTO Animals (name, wild) VALUES (%s, %s)", (name, wild))
        conn.commit()
        return Animals(name=name, wild=wild)
    
    @strawberry.mutation
    def update_animals(self, id: str, name: str, wild: str) -> Animals:
        cursor.execute("UPDATE Animals SET name=%s, wild=%s WHERE id = %s", (name, wild, id))
        conn.commit()
        return Animals(name=name, wild=wild)
    
    @strawberry.mutation
    def delete_animals(self, id: str) -> Animals:
        cursor.execute("DELETE FROM Animals WHERE id = %s", (id,))
        conn.commit()
        return Animals(name=name, wild=wild)
    
    @strawberry.mutation
    def create_birds(self, name: str, family: str) -> Birds:
        cursor.execute("INSERT INTO Birds (name, family) VALUES (%s, %s)", (name, family))
        conn.commit()
        return Birds(name=name, family=family)
    
    @strawberry.mutation
    def update_birds(self, id: str, name: str, family: str) -> Birds:
        cursor.execute("UPDATE Birds SET name=%s, family=%s WHERE id = %s", (name, family, id))
        conn.commit()
        return Birds(name=name, family=family)
    
    @strawberry.mutation
    def delete_birds(self, id: str) -> Birds:
        cursor.execute("DELETE FROM Birds WHERE id = %s", (id,))
        conn.commit()
        return Birds(name=name, family=family)
    

schema = strawberry.Schema(Query, Mutation)


graphql_app = GraphQLRouter(
    schema,
)


app = FastAPI()
app.include_router(graphql_app, prefix="/graphql")

# main function 
if __name__ == "__main__": 
    try:
        main()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    print(os.environ['PG_HOST'] , os.environ['PG_DATABASE'] , os.environ['PG_USER'] , os.environ['PG_PASSWORD']) 
    uvicorn.run(app, host='0.0.0.0', port=8000)
