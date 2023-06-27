import strawberry
import uvicorn
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
import typing 
import psycopg2
import os
from fastapi.middleware.cors import CORSMiddleware
import time

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
    cursor.execute('CREATE TABLE IF NOT EXISTS Employee (id SERIAL PRIMARY KEY, e_id INT, name VARCHAR, email VARCHAR, phone INT, age INT, salary DECIMAL)') 
    conn.commit() 
    cursor.execute('CREATE TABLE IF NOT EXISTS Sample (id SERIAL PRIMARY KEY, word VARCHAR(255))') 
    conn.commit() 
    print("Table created successfully")

#*Dataclasses
@strawberry.type
class Employee:
    id: str
    e_id: str
    name: str
    email: str
    phone: str
    age: str
    salary: str

@strawberry.type
class Sample:
    id : str 
    word : str

@strawberry.type
class Query:
    #*graphquery
    @strawberry.field
    def all_employee(self) -> typing.List[Employee]:
        cursor.execute("SELECT * FROM Employee")
        lst = cursor.fetchall()
        employee = []
        for i in lst:
            employee.append(Employee(id=i[0], e_id=i[1], name=i[2], email=i[3], phone=i[4], age=i[5], salary=i[6]))
        return employee

    @strawberry.field
    def get_employee(self, id: str) -> Employee:
        cursor.execute("SELECT * FROM Employee WHERE id = %s", (id,))
        lst = cursor.fetchone()
        return Employee(id=lst[0], e_id=lst[1], name=lst[2], email=lst[3], phone=lst[4], age=lst[5], salary=lst[6])
         

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
    def create_employee(self, e_id: str, name: str, email: str, phone: str, age: str, salary: str) -> Employee:
        cursor.execute("INSERT INTO Employee (e_id, name, email, phone, age, salary) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id", (e_id, name, email, phone, age, salary))
        conn.commit()
        employee_id = cursor.fetchone()[0]
        return Employee(id=employee_id,e_id=e_id, name=name, email=email, phone=phone, age=age, salary=salary)
    
    @strawberry.mutation
    def update_employee(self, id: str, e_id: str, name: str, email: str, phone: str, age: str, salary: str) -> Employee:
        
        cursor.execute("UPDATE Employee SET e_id=%s, name=%s, email=%s, phone=%s, age=%s, salary=%s WHERE id = %s", (e_id, name, email, phone, age, salary, id))
        conn.commit()
        return Employee(id=id,e_id=e_id, name=name, email=email, phone=phone, age=age, salary=salary)
    
    @strawberry.mutation
    def delete_employee(self, id: str) -> Employee:
        cursor.execute("SELECT * FROM Employee WHERE id = %s", (id,))
        lst = cursor.fetchone()
        if lst is None:
            return Employee(id='No Data Found',e_id='No Data Found', name='No Data Found', email='No Data Found', phone='No Data Found', age='No Data Found', salary='No Data Found')

        cursor.execute("DELETE FROM Employee WHERE id = %s", (id,))
        conn.commit()
        return Employee(id=lst[0], e_id=lst[1], name=lst[2], email=lst[3], phone=lst[4], age=lst[5], salary=lst[6])
     

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