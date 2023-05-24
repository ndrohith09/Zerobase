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
    cursor.execute(CREATE TABLE IF NOT EXISTS Animals (id SERIAL PRIMARY KEY, name VARCHAR(255), family VARCHAR(255))) 
    conn.commit()

    cursor.execute(CREATE TABLE IF NOT EXISTS Birds (id SERIAL PRIMARY KEY, name VARCHAR(255), family VARCHAR(255))) 
    conn.commit()

    
    print("Inside main")

#*Dataclasses
@strawberry.type
class Animals:
    id: str
    name: str
    family: str

@strawberry.type
class Birds:
    id: str
    name: str
    family: str


    
@strawberry.type
class Query:
    
    
    
    #*graphquery
    @strawberry.field
    async def all_animals(self) -> typing.List[Animals]:
        cursor.execute("SELECT * FROM Animals")
        lst = cursor.fetchall()
        animals = []
        for i in lst:
            animals.append(Animals(id=i[0], name=i[1], family=i[2]))
        return animals

    @strawberry.field
    async def get_animals(self, id: str) -> Animals:
        cursor.execute("SELECT * FROM Animals WHERE id = %s", (id,))
        lst = cursor.fetchone()
        return Animals(id=lst[0], name=lst[1], family=lst[2])

    
    @strawberry.field
    async def all_birds(self) -> typing.List[Birds]:
        cursor.execute("SELECT * FROM Birds")
        lst = cursor.fetchall()
        birds = []
        for i in lst:
            birds.append(Birds(id=i[0], name=i[1], family=i[2]))
        return birds

    @strawberry.field
    async def get_birds(self, id: str) -> Birds:
        cursor.execute("SELECT * FROM Birds WHERE id = %s", (id,))
        lst = cursor.fetchone()
        return Birds(id=lst[0], name=lst[1], family=lst[2])

        

@strawberry.type
class Mutation:

    @strawberry.mutation 
    def add_book(self, title: str, instructor: str, publish_date: str) -> Book:
        # add data to postgres database 
        cursor.execute( 
            "INSERT INTO books (title, instructor, publish_date) VALUES (%s, %s, %s) RETURNING id",
            (title, instructor, publish_date)
        )
        book_id = cursor.fetchone()[0]
        conn.commit()
        return Book(id=book_id, title=title, instructor=instructor, publish_date=publish_date)

    @strawberry.mutation 
    def update_book(self, id: str, title: str, instructor: str, publish_date: str) -> Book:

        # query old data 
        cursor.execute("SELECT * FROM books WHERE id = %s", (id,))
        course_list = cursor.fetchone()
        print(course_list)
        if course_list is None:
            return Book(id="0", title="No book found", instructor="No book found", publish_date="No book found")

        if title == "" or title is None:
            title = course_list[1]
        if instructor == "" or instructor is None:
            instructor = course_list[2] 
        if publish_date == "" or publish_date is None:
            publish_date = course_list[3]

        cursor.execute( 
            "UPDATE books SET title = %s, instructor = %s, publish_date = %s WHERE id = %s",
            (title, instructor, publish_date, id)
        )
        conn.commit()
        return Book(id=id, title=title, instructor=instructor, publish_date=publish_date)

    @strawberry.mutation
    def delete_book(self, id: str) -> Book:
        # query old data 
        cursor.execute("SELECT * FROM books WHERE id = %s", (id,))
        course_list = cursor.fetchone()
        print(course_list)
        if course_list is None:
            return Book(id="0", title="No book found", instructor="No book found", publish_date="No book found")

        cursor.execute("DELETE FROM books WHERE id = %s", (id,))
        conn.commit()
        return Book(id=id, title=course_list[1], instructor=course_list[2], publish_date=course_list[3])

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
