import pandas as pd

# File name


class SqlServer:
    def __init__(self,file_name):
        # Types of dat
        self.file_name = file_name
        self.VARCHAR = 'VARCHAR(250)'
        self.FLOAT = 'FLOAT'

    def infer_sql_type(self, series):
        if pd.api.types.is_string_dtype(series):
            return "VARCHAR(255)"
        elif pd.api.types.is_integer_dtype(series):
            return "INT"
        elif pd.api.types.is_float_dtype(series):
            return "DECIMAL(15,2)"
        elif pd.api.types.is_datetime64_any_dtype(series):
            # Because I used MySQL
            return "DATETIME"
        else:
            return "VARCHAR(255)"


    def create_table(self,df_new, table_name):
        TABLE_NAME = table_name
        df_new = df_new
        # ---------- CREATE TABLE
        list_title = []
        list_original_title = []
        # ----- Collect the columns title in the CSV file
        for column_title in df_new:
            # Checking the titles of columns in CSV file
            # Replace special characters in title of row and pass to lower case
            split_str = str(column_title).replace('(','')
            split_str = split_str.replace(')','')
            correct_str = split_str.lower().split()
            list_original_title.append(column_title)

            # Join separate words with '_'
            list_title.append('_'.join(correct_str))

        print(list_title)

        # Creating the table
        with open(self.file_name, 'a') as file:
            # Writing CREATE TABLE in SQL file
            file.write(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME}( ")
            # Creating the title of rows in table
            for index in range(len(list_original_title)):
                print(list_title[index])
                # Check the type data and writing the row of TABLE
                col = df_new[list_original_title[index]]
                sql_type = self.infer_sql_type(col)
                if index == 0:
                    file.write(f'\n{list_title[index]} {sql_type}')
                else:
                    file.write(f',\n{list_title[index]} {sql_type}')

            file.write(');\n\n')
        self.insert_values(df=df_new,table_name=table_name, list_titles=list_title)

    def insert_values(self,df,table_name,list_titles):
        values_list = []
        print('Inserting values...')
        for j in range(len(df)):
            for i in df.loc[j]:
                # Checking the type of values
                if type(i) == str:
                    # Checkin if the value is NULL
                    if i == "NULL":
                        values_list.append(f"{i}")
                    elif i == "" or i == 0:
                        values_list.append("NULL")
                    else:
                        values_list.append(f"'{i}'")
                else:
                    values_list.append(f"{i}")
            with open(self.file_name, 'a') as my_file:
                my_file.write(f'INSERT INTO {table_name}({','.join(list_titles)}) VALUES({', '.join(values_list)});\n')

            values_list = []
        print("Insertion successfully")
