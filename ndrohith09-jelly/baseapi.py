import strawberry
import uvicorn
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
import typing 
import psycopg2
import os
from fastapi.middleware.cors import CORSMiddleware

def establish_connection():
    conn = None
    retry_attempts = 5
    retry_delay = 2

    for attempt in range(retry_attempts):
        try:
            conn = psycopg2.connect(os.environ['DATABASE_URL'])
            conn.autocommit = True
            return conn

        except (psycopg2.OperationalError, psycopg2.Error) as e:
            print(f"Connection attempt {attempt + 1} failed: {str(e)}")
            time.sleep(retry_delay)

    raise Exception("Failed to establish a connection to the PostgreSQL database")

try:
    conn = establish_connection() 
    cursor = conn.cursor()

    print("Connected to the PostgreSQL database")

except (Exception, psycopg2.DatabaseError) as error:
    print("Cannot connect to the PostgreSQL database")
    print(error) 

#*main
def main():
    cursor.execute('CREATE TABLE IF NOT EXISTS Fish (id SERIAL PRIMARY KEY, breed VARCHAR(200))') 
    conn.commit() 
    cursor.execute('CREATE TABLE IF NOT EXISTS Sample (id SERIAL PRIMARY KEY, word VARCHAR(255))') 
    conn.commit() 
    print("Table created successfully")

#*Dataclasses
@strawberry.type
class Fish:
    id: str
    breed: str

@strawberry.type
class Sample:
    id : str 
    word : str

@strawberry.type
class Query:
    #*graphquery
    @strawberry.field
    def all_fish(self) -> typing.List[Fish]:
        cursor.execute("SELECT * FROM Fish")
        lst = cursor.fetchall()
        fish = []
        for i in lst:
            fish.append(Fish(id=i[0], breed=i[1]))
        return fish

    @strawberry.field
    def get_fish(self, id: str) -> Fish:
        cursor.execute("SELECT * FROM Fish WHERE id = %s", (id,))
        lst = cursor.fetchone()
        return Fish(id=lst[0], breed=lst[1])
         

    @strawberry.field
    def all_sample(self) -> typing.List[Sample]:
        cursor.execute("SELECT * FROM Sample")
        lst = cursor.fetchall()
        sample = []
        for i in lst:
            sample.append(Sample(id=i[0], word=i[1]))
        return sample

@strawberry.type
class Mutation:
    #*graphmutation
    @strawberry.mutation
    def create_fish(self, breed: str) -> Fish:
        cursor.execute("INSERT INTO Fish (breed) VALUES (%s) RETURNING id", (breed))
        conn.commit()
        fish_id = cursor.fetchone()[0]
        return Fish(id=fish_id,breed=breed)
    
    @strawberry.mutation
    def update_fish(self, id: str, breed: str) -> Fish:
        
        cursor.execute("UPDATE Fish SET breed=%s WHERE id = %s", (breed, id))
        conn.commit()
        return Fish(id=id,breed=breed)
    
    @strawberry.mutation
    def delete_fish(self, id: str) -> Fish:
        cursor.execute("SELECT * FROM Fish WHERE id = %s", (id,))
        lst = cursor.fetchone()
        if lst is None:
            return Fish(id='No Data Found',breed='No Data Found')

        cursor.execute("DELETE FROM Fish WHERE id = %s", (id,))
        conn.commit()
        return Fish(id=lst[0], breed=lst[1])
     

    @strawberry.mutation
    def create_sample(self, word: str) -> Sample:
        print(word , type(word))
        cursor.execute("INSERT INTO Sample (word) VALUES (%s) RETURNING id", (word,))
        conn.commit()
        samples_id = cursor.fetchone()[0]
        return Sample(id=samples_id,word=word)

schema = strawberry.Schema(Query , Mutation)


graphql_app = GraphQLRouter(
    schema,
)


app = FastAPI()
app.include_router(graphql_app, prefix="/graphql")

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"], 
)

if __name__ == "__main__": 
    try:
        main()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error) 
    uvicorn.run(app, host='0.0.0.0', port=8000)
