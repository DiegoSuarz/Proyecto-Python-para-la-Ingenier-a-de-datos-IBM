import sqlite3
import pandas as pd

conn = sqlite3.connect('STAFF.db')

table_name = 'Departaments'
#attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']
attribute_list = ['DEPT_ID', 'DEP_NAME', 'MANAGER_ID', 'LOC_ID']


file_path = '/home/project/database2/Departments.csv'
df = pd.read_csv(file_path, names = attribute_list)

# CREAR LA TABLA INSTRUCTOR
df.to_sql(table_name, conn, if_exists = 'replace', index =False)
print('Table is ready')

## Realizar consulta a la base de datos
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)


## Seleccionar la columna Departamentos:
query_statement = f"SELECT DEP_NAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)


## Ver la cantidad total de entradas en la tabla:
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

## Crear nuevos datos:
data_dict = {
            'DEPT_ID' : [9],
            'DEP_NAME' : ['Aseguramiento de Calidad'],
            'MANAGER_ID' : [30010],
            'LOC_ID' : ['L0010']
            }
data_append = pd.DataFrame(data_dict)

## Agregar los datos creados a la tabla:
data_append.to_sql(table_name, conn, if_exists = 'append', index =False)
print('Data appended successfully')

## Realizar consulta a la base de datos para ver los datos agregados
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

## Cerramos la conexion a la base de datos:
conn.close()