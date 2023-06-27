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
    cursor.execute('CREATE TABLE IF NOT EXISTS Insurance (id SERIAL PRIMARY KEY, insurance_id INT, insurance_type VARCHAR, e_id INT)') 
    conn.commit()
    cursor.execute('CREATE TABLE IF NOT EXISTS Department (id SERIAL PRIMARY KEY, d_id INT, name VARCHAR, manager_id INT)') 
    conn.commit()
    cursor.execute('CREATE TABLE IF NOT EXISTS Employee (id SERIAL PRIMARY KEY, e_id INT, name VARCHAR, age INT, phone INT, email VARCHAR, salary DECIMAL)') 
    conn.commit() 
    cursor.execute('CREATE TABLE IF NOT EXISTS Sample (id SERIAL PRIMARY KEY, word VARCHAR(255))') 
    conn.commit() 
    print("Table created successfully")

#*Dataclasses
@strawberry.type
class Insurance:
    id: str
    insurance_id: str
    insurance_type: str
    e_id: str

@strawberry.type
class Department:
    id: str
    d_id: str
    name: str
    manager_id: str

@strawberry.type
class Employee:
    id: str
    e_id: str
    name: str
    age: str
    phone: str
    email: str
    salary: str

@strawberry.type
class Sample:
    id : str 
    word : str

@strawberry.type
class Query:
    #*graphquery
    @strawberry.field
    def all_insurance(self) -> typing.List[Insurance]:
        cursor.execute("SELECT * FROM Insurance")
        lst = cursor.fetchall()
        insurance = []
        for i in lst:
            insurance.append(Insurance(id=i[0], insurance_id=i[1], insurance_type=i[2], e_id=i[3]))
        return insurance

    @strawberry.field
    def get_insurance(self, id: str) -> Insurance:
        cursor.execute("SELECT * FROM Insurance WHERE id = %s", (id,))
        lst = cursor.fetchone()
        return Insurance(id=lst[0], insurance_id=lst[1], insurance_type=lst[2], e_id=lst[3])
    
    @strawberry.field
    def all_department(self) -> typing.List[Department]:
        cursor.execute("SELECT * FROM Department")
        lst = cursor.fetchall()
        department = []
        for i in lst:
            department.append(Department(id=i[0], d_id=i[1], name=i[2], manager_id=i[3]))
        return department

    @strawberry.field
    def get_department(self, id: str) -> Department:
        cursor.execute("SELECT * FROM Department WHERE id = %s", (id,))
        lst = cursor.fetchone()
        return Department(id=lst[0], d_id=lst[1], name=lst[2], manager_id=lst[3])
    
    @strawberry.field
    def all_employee(self) -> typing.List[Employee]:
        cursor.execute("SELECT * FROM Employee")
        lst = cursor.fetchall()
        employee = []
        for i in lst:
            employee.append(Employee(id=i[0], e_id=i[1], name=i[2], age=i[3], phone=i[4], email=i[5], salary=i[6]))
        return employee

    @strawberry.field
    def get_employee(self, id: str) -> Employee:
        cursor.execute("SELECT * FROM Employee WHERE id = %s", (id,))
        lst = cursor.fetchone()
        return Employee(id=lst[0], e_id=lst[1], name=lst[2], age=lst[3], phone=lst[4], email=lst[5], salary=lst[6])
         

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
    def create_insurance(self, insurance_id: str, insurance_type: str, e_id: str) -> Insurance:
        cursor.execute("INSERT INTO Insurance (insurance_id, insurance_type, e_id) VALUES (%s, %s, %s) RETURNING id", (insurance_id, insurance_type, e_id))
        conn.commit()
        insurance_id = cursor.fetchone()[0]
        return Insurance(id=insurance_id,insurance_id=insurance_id, insurance_type=insurance_type, e_id=e_id)
    
    @strawberry.mutation
    def update_insurance(self, id: str, insurance_id: str, insurance_type: str, e_id: str) -> Insurance:
        
        cursor.execute("UPDATE Insurance SET insurance_id=%s, insurance_type=%s, e_id=%s WHERE id = %s", (insurance_id, insurance_type, e_id, id))
        conn.commit()
        return Insurance(id=id,insurance_id=insurance_id, insurance_type=insurance_type, e_id=e_id)
    
    @strawberry.mutation
    def delete_insurance(self, id: str) -> Insurance:
        cursor.execute("SELECT * FROM Insurance WHERE id = %s", (id,))
        lst = cursor.fetchone()
        if lst is None:
            return Insurance(id='No Data Found',insurance_id='No Data Found', insurance_type='No Data Found', e_id='No Data Found')

        cursor.execute("DELETE FROM Insurance WHERE id = %s", (id,))
        conn.commit()
        return Insurance(id=lst[0], insurance_id=lst[1], insurance_type=lst[2], e_id=lst[3])
    
    @strawberry.mutation
    def create_department(self, d_id: str, name: str, manager_id: str) -> Department:
        cursor.execute("INSERT INTO Department (d_id, name, manager_id) VALUES (%s, %s, %s) RETURNING id", (d_id, name, manager_id))
        conn.commit()
        department_id = cursor.fetchone()[0]
        return Department(id=department_id,d_id=d_id, name=name, manager_id=manager_id)
    
    @strawberry.mutation
    def update_department(self, id: str, d_id: str, name: str, manager_id: str) -> Department:
        
        cursor.execute("UPDATE Department SET d_id=%s, name=%s, manager_id=%s WHERE id = %s", (d_id, name, manager_id, id))
        conn.commit()
        return Department(id=id,d_id=d_id, name=name, manager_id=manager_id)
    
    @strawberry.mutation
    def delete_department(self, id: str) -> Department:
        cursor.execute("SELECT * FROM Department WHERE id = %s", (id,))
        lst = cursor.fetchone()
        if lst is None:
            return Department(id='No Data Found',d_id='No Data Found', name='No Data Found', manager_id='No Data Found')

        cursor.execute("DELETE FROM Department WHERE id = %s", (id,))
        conn.commit()
        return Department(id=lst[0], d_id=lst[1], name=lst[2], manager_id=lst[3])
    
    @strawberry.mutation
    def create_employee(self, e_id: str, name: str, age: str, phone: str, email: str, salary: str) -> Employee:
        cursor.execute("INSERT INTO Employee (e_id, name, age, phone, email, salary) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id", (e_id, name, age, phone, email, salary))
        conn.commit()
        employee_id = cursor.fetchone()[0]
        return Employee(id=employee_id,e_id=e_id, name=name, age=age, phone=phone, email=email, salary=salary)
    
    @strawberry.mutation
    def update_employee(self, id: str, e_id: str, name: str, age: str, phone: str, email: str, salary: str) -> Employee:
        
        cursor.execute("UPDATE Employee SET e_id=%s, name=%s, age=%s, phone=%s, email=%s, salary=%s WHERE id = %s", (e_id, name, age, phone, email, salary, id))
        conn.commit()
        return Employee(id=id,e_id=e_id, name=name, age=age, phone=phone, email=email, salary=salary)
    
    @strawberry.mutation
    def delete_employee(self, id: str) -> Employee:
        cursor.execute("SELECT * FROM Employee WHERE id = %s", (id,))
        lst = cursor.fetchone()
        if lst is None:
            return Employee(id='No Data Found',e_id='No Data Found', name='No Data Found', age='No Data Found', phone='No Data Found', email='No Data Found', salary='No Data Found')

        cursor.execute("DELETE FROM Employee WHERE id = %s", (id,))
        conn.commit()
        return Employee(id=lst[0], e_id=lst[1], name=lst[2], age=lst[3], phone=lst[4], email=lst[5], salary=lst[6])
     

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