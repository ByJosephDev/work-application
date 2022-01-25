import datetime
from tkinter.ttk import Combobox

import pandas as pd
from tkinter import ttk, filedialog
from tkinter import *
import pyodbc
from tkcalendar import DateEntry

""" Connections """

# svr_name = 'DESKTOP-BHB4DRO'
# db_name = 'cye'

# conn_str = (
#         r'Driver={ODBC Driver 17 for SQL Server};'  # Just an Example (SQL2008-2018)
#         r'Server=' + svr_name + ';'  # Here you insert you servername
#                                 r'Database=' + db_name + ';'  # Here you insert your db Name
#                                                          r'Trusted_Connection=yes;'
#     # This flag enables windows authentication
# )

driver = 'SQL Server'
server = 'DESKTOP-BHB4DRO'
db = 'cye'
user = 'sa'
password = 'root'


def run_query_get_data(query, parameters=()):

    # conn = pyodbc.connect(conn_str)

    conn = pyodbc.connect('driver={%s};server=%s;database=%s;uid=%s;pwd=%s' % (driver, server, db, user, password))
    cursor = conn.cursor()

    try:

        result = cursor.execute(query, parameters)
        return result

    except Exception as e:

        print(f'{e}')


def run_query(query, parameters=()):

    # conn = pyodbc.connect(conn_str)

    conn = pyodbc.connect('driver={%s};server=%s;database=%s;uid=%s;pwd=%s' % (driver, server, db, user, password))
    cursor = conn.cursor()

    try:

        cursor.execute(query, parameters)
        conn.commit()

    except Exception as exc:

        print(f'{exc}')

    finally:

        cursor.close()
        conn.close()


""" Functions """


def excel_to_sql_program(id_program, date_begin, date_end, url):
    try:

        """ Read Excel and convert to Data Frame """

        dataframe = pd.read_excel(url)

        """  DELETE COLUMNS from Data Frame  """

        dataframe.drop(dataframe.columns[0], axis=1, inplace=True)
        dataframe.drop(dataframe.columns[0], axis=1, inplace=True)
        dataframe.drop(dataframe.columns[0], axis=1, inplace=True)
        dataframe.drop(dataframe.columns[0], axis=1, inplace=True)
        dataframe.drop(dataframe.columns[0], axis=1, inplace=True)

        """ Modify Data Frame  """

        dataframe.columns = ['pcolaborador_dni', 'p1', 'p2', 'p3']

        dataframe.fillna('', inplace=True)

        dataframe.insert(4, "programa_id", int(id_program))
        dataframe.insert(5, 'fecha_inicio', date_begin)
        dataframe.insert(6, 'fecha_fin', date_end)

        """ Data Frame TO SQL SERVER"""

        query = "DELETE FROM cye.encuesta_programa WHERE programa_id = ? AND fecha_inicio = ? AND fecha_fin = ?"
        parameters = (int(dataframe.iloc[0][4]), dataframe.iloc[0][5], dataframe.iloc[0][6])
        run_query(query, parameters)

        for row in dataframe.itertuples():
            query2 = "DELETE FROM cye.encuesta_programa WHERE pcolaborador_dni = ? " \
                     "AND programa_id = ? AND fecha_inicio = ? AND fecha_fin = ?"
            parameters2 = (row.pcolaborador_dni, row.programa_id, row.fecha_inicio, row.fecha_fin)
            run_query(query2, parameters2)

            query3 = "INSERT INTO cye.encuesta_programa VALUES(?,?,?,?,?,?,?)"
            parameters3 = (row.pcolaborador_dni, row.p1, row.p2, row.p3
                           , row.programa_id, row.fecha_inicio, row.fecha_fin)
            run_query(query3, parameters3)

        return True

    except Exception as e:

        print(f'{e}')
        return False


def excel_to_sql_experience(url):
    try:

        """Read Excel and convert to Data Frame"""

        dataframe = pd.read_excel(url)

        """   DELETE COLUMNS from Data Frame """

        dataframe.drop(dataframe.columns[0], axis=1, inplace=True)
        dataframe.drop(dataframe.columns[0], axis=1, inplace=True)
        dataframe.drop(dataframe.columns[0], axis=1, inplace=True)
        dataframe.drop(dataframe.columns[0], axis=1, inplace=True)
        dataframe.drop(dataframe.columns[0], axis=1, inplace=True)

        """   Modify Dataframe"""

        dataframe.columns = ['nombre', 'pcolaborador_dni', 'area', 'p1', 'fecha', 'p2', 'p3', 'p4', 'p5',
                             'p6', 'p7', 'p8', 'p9', 'p10', 'p11', 'p12', 'p13', 'p14', 'p15', 'p16', 'p17']

        dataframe.drop(['nombre', 'area', 'fecha'], axis=1, inplace=True)

        dataframe.fillna('', inplace=True)

        """Data Frame TO SQL SERVER - EXP"""

        for row in dataframe.itertuples():
            query = "DELETE FROM cye.encuesta_experiencia WHERE pcolaborador_dni = ?"
            parameter = row.pcolaborador_dni
            run_query(query, parameter)

            query2 = "INSERT INTO cye.encuesta_experiencia VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            parameters = (row.pcolaborador_dni, row.p1, row.p2, row.p3, row.p4, row.p5
                          , row.p6, row.p7, row.p8, row.p9, row.p10, row.p11, row.p12
                          , row.p13, row.p14, row.p15, row.p16, row.p17)

            run_query(query2, parameters)

        return True

    except Exception as e:

        print(f'{e}')
        return False


def excel_to_sql_supervisor(url):
    try:

        """ Read Excel and convert to Data Frame """

        dataframe = pd.read_excel(url)

        """ DELETE COLUMNS from Data Frame """

        dataframe.drop(dataframe.columns[0], axis=1, inplace=True)
        dataframe.drop(dataframe.columns[0], axis=1, inplace=True)
        dataframe.drop(dataframe.columns[0], axis=1, inplace=True)
        dataframe.drop(dataframe.columns[0], axis=1, inplace=True)
        dataframe.drop(dataframe.columns[0], axis=1, inplace=True)

        """ Modify Dataframe """

        dataframe.columns = ['nombre_supervisor', 'area', 'nombre_trabajador', 'pcolaborador_dni', 'p1', 'p2', 'p3',
                             'p4', 'p5',
                             'p6']

        dataframe.drop(['area', 'nombre_trabajador'], axis=1, inplace=True)

        dataframe.fillna('', inplace=True)

        """Data Frame TO SQL SERVER - EXP"""

        for row in dataframe.itertuples():
            query = "DELETE FROM cye.encuesta_supervisor WHERE pcolaborador_dni = ?"
            parameter = row.pcolaborador_dni
            run_query(query, parameter)

            query2 = "INSERT INTO cye.encuesta_supervisor VALUES(?,?,?,?,?,?,?,?)"
            parameters = (row.nombre_supervisor, row.pcolaborador_dni
                          , row.p1, row.p2, row.p3, row.p4, row.p5
                          , row.p6)
            run_query(query2, parameters)

        return True

    except Exception as ex:

        print(f'{ex}')
        return False


""" Application """


class Application:
    """ Graphic User Interface """

    def __init__(self, window_root):

        self.wind = window_root
        self.wind.resizable(0, 0)
        self.wind.title("Application")

        """ CONTAINER TOP """

        # container TOP
        container_top = Frame()
        container_top.pack(side="top", anchor="w")

        # creating a frame container program
        frame = LabelFrame(container_top, text="Registrar un nuevo programa")
        frame.grid(row=0, column=0, padx=10)

        # name of program input
        Label(frame, text='Programa: ').grid(row=0, column=0)
        self.name_program = Entry(frame)
        self.name_program.grid(row=0, column=1, padx=5, pady=5)

        # id category input
        Label(frame, text='Categoria: ').grid(row=1, column=0)
        self.var = StringVar()
        self.data = ("EMPLEADO", "PRACTICANTE", "FUNCIONARIO", "OBRERO")
        self.category = Combobox(frame, values=self.data, state="readonly")
        self.category.grid(row=1, column=1, padx=5, pady=5)

        # button Add Program
        ttk.Button(frame, text='Registrar programa', command=self.create_program) \
            .grid(row=2, column=0, columnspan=3, padx=10, pady=10)

        # creating a frame container Excel Quest program

        frame1 = LabelFrame(container_top, text="Importar excel de programas")
        frame1.grid(row=0, column=1, padx=10, pady=10)

        # ID of program input
        Label(frame1, text='ID programa: ').grid(row=0, column=0)
        self.id_program = Entry(frame1)
        self.id_program.grid(row=0, column=1, padx=5, pady=5)

        # Date begin input
        Label(frame1, text='Fecha de inicio: ').grid(row=1, column=0)
        self.date_begin = DateEntry(frame1, locale='en_US', date_pattern='dd/mm/y')
        self.date_begin.grid(row=1, column=1, padx=5, pady=5)

        # Date end input
        Label(frame1, text='Fecha de finalización: ').grid(row=2, column=0)
        self.date_end = DateEntry(frame1, locale='en_US', date_pattern='dd/mm/y')
        self.date_end.grid(row=2, column=1, padx=5, pady=5)

        # Button Import Excel Programs
        ttk.Button(frame1, text='Importar Excel', command=self.import_excel_to_sql_program) \
            .grid(row=3, column=0, columnspan=3, padx=10, pady=10)

        # creating a frame container Excel EXP -SUP

        frame2 = LabelFrame(container_top, text="Importar encuestas")
        frame2.grid(row=0, column=2, padx=10, pady=10)

        # Button Import Excel EXP
        ttk.Button(frame2, text='Importar Excel - Experiencia', command=self.import_excel_to_sql_experience) \
            .grid(row=3, column=0, columnspan=3, padx=10, pady=10)

        # Button Import Excel SUPER
        ttk.Button(frame2, text='Importar Excel - Supervisor', command=self.import_excel_to_sql_supervisor) \
            .grid(row=4, column=0, columnspan=3, padx=10, pady=10)

        """ CONTAINER BOTTOM """

        # Container BOTTOM

        container_bottom = Frame()
        container_bottom.pack(side="bottom", pady=20)

        # Button get programs
        ttk.Button(container_bottom, text='Consultar programas',
                   command=self.get_programs) \
            .grid(row=0, column=0, columnspan=2)

        self.tree1 = ttk.Treeview(container_bottom, columns=("A", "B"))
        self.tree1.grid(row=1, column=0, columnspan=1, padx=10, pady=20)
        self.tree1.heading("#0", text="ID")
        self.tree1.column("#0", minwidth=0, width=50, anchor=CENTER)
        self.tree1.heading("A", text="Nombre de programa")
        self.tree1.column("A", minwidth=0, width=250, anchor=CENTER)
        self.tree1.heading("B", text="Categoria")
        self.tree1.column("B", minwidth=0, width=150, anchor=CENTER)

        # OUTPUT MESSAGES
        self.message = Label(container_bottom, text='', fg='red', font="Arial 19")
        self.message.grid(row=2, column=0, columnspan=2, sticky=W + E, pady=10)

    """ Constraints """

    def validation_fields_create_program(self):

        return len(self.name_program.get()) != 0 \
               and len(self.category.get()) != 0

    def validation_category(self):

        return self.category == 'EMPLEADO' or self.category == 'FUNCIONARIO' \
               or self.category == 'PRACTICANTE' or self.category == 'OBRERO'

    def validation_fields_excel_program(self):

        return len(self.id_program.get()) != 0

    def validation_exist_id_program(self):

        query = " SELECT id  FROM cye.programa WHERE id=?"
        parameter = int(self.id_program.get())

        db_rows = run_query_get_data(query, parameter)

        lf_id = 0

        for row in db_rows:
            lf_id = row[0]

        if int(self.id_program.get()) == lf_id:

            db_rows.close()
            return True

        else:

            db_rows.close()
            return False

    """ GET TABLES """

    def get_programs(self):

        records = self.tree1.get_children()

        for element in records:
            self.tree1.delete(element)

        query = 'SELECT * FROM cye.programa ORDER BY id DESC'
        db_rows = run_query_get_data(query)
        for row in db_rows:
            self.tree1.insert('', 0, text=row[0], values=(row[1], row[2]))

    """ Button functions"""

    def create_program(self):

        if self.validation_fields_create_program():

            try:

                query = "INSERT INTO cye.programa VALUES (?,?)"
                parameters = (self.name_program.get(), self.category.get())

                run_query(query, parameters)
                self.get_programs()
                self.message['text'] = 'Registrado correctamente!!'
                self.message['fg'] = 'green'

            except Exception as e:

                self.message['text'] = 'Error al registrar'
                self.message['fg'] = 'red'
                print(f'{e}')

        else:

            self.message['text'] = 'Parametros vacios'
            self.message['fg'] = 'orange'

    def import_excel_to_sql_program(self):

        if self.validation_fields_excel_program():

            try:

                if self.validation_exist_id_program() and int(self.id_program.get()) > 0:

                    url_file = filedialog.askopenfilename(title="buscar archivo", initialdir="C:/",
                                                          filetypes=[("xlsx files", ".xlsx"),
                                                                     ("xls files", ".xls")])

                    date_begin = datetime.datetime.strptime(self.date_begin.get(), '%d/%m/%Y')
                    date_end = datetime.datetime.strptime(self.date_end.get(), '%d/%m/%Y')

                    new_date_begin = datetime.date.strftime(date_begin, "%Y/%m/%d")
                    new_date_end = datetime.date.strftime(date_end, "%Y/%m/%d")

                    if excel_to_sql_program(self.id_program.get(), new_date_begin, new_date_end, url_file):

                        self.message['text'] = 'Registrado correctamente!!'
                        self.message['fg'] = 'green'

                    else:

                        self.message['text'] = 'Error al registrar'
                        self.message['fg'] = 'red'

                else:

                    self.message['text'] = 'Error en el ID'
                    self.message['fg'] = 'red'

            except Exception as e:

                self.message['text'] = 'No se permite letras en el id'
                self.message['fg'] = 'red'
                print(f'{e}')

        else:

            self.message['text'] = 'Ingrese el id del programa'
            self.message['fg'] = 'orange'

    def import_excel_to_sql_experience(self):

        url_file = filedialog.askopenfilename(title="buscar archivo", initialdir="C:/",
                                              filetypes=[("xlsx files", ".xlsx"), ("xls files", ".xls")])

        if excel_to_sql_experience(url_file):

            self.message['text'] = 'Se importo correctamente '
            self.message['fg'] = 'green'

        else:

            self.message['text'] = 'Error al importar el excel '
            self.message['fg'] = 'red'

    def import_excel_to_sql_supervisor(self):

        url_file = filedialog.askopenfilename(title="abrir archivo", initialdir="C:/",
                                              filetypes=[("xlsx files", ".xlsx"), ("xls files", ".xls")])

        if excel_to_sql_supervisor(url_file):

            self.message['text'] = 'Se importo correctamente'
            self.message['fg'] = 'green'

        else:

            self.message['text'] = 'Error al importar el excel '
            self.message['fg'] = 'red'


""" EXECUTE Application """

if __name__ == '__main__':
    window = Tk()
    app = Application(window)
    window.mainloop()
