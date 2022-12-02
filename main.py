from Utilities.utilities import *
from initTables import *
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtWidgets import QFileDialog, QMessageBox
from PyQt5.QtCore import QDir
from Utilities.utilities import createSchema, fromCSV, insertData, printTable, deleteTables
import workload as wl
from initTables import initTables
import sqlite3 as sq

db_path = CURR_PATH + '/Data/e2cDB.db'
conn = sq.connect(db_path)
cur = conn.cursor()
class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(796, 613)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.scen_label = QtWidgets.QLabel(self.centralwidget)
        self.scen_label.setGeometry(QtCore.QRect(490, 60, 91, 16))
        self.scen_label.setObjectName("scen_label")
        self.scen_load = QtWidgets.QPushButton(self.centralwidget)
        self.scen_load.setGeometry(QtCore.QRect(490, 120, 75, 23))
        self.scen_load.setObjectName("scen_load")
        self.scen_path = QtWidgets.QLineEdit(self.centralwidget)
        self.scen_path.setGeometry(QtCore.QRect(490, 90, 221, 20))
        self.scen_path.setObjectName("scen_path")
        self.scen_path.setText("(path to scenario here)")
        self.scen_path.setEnabled(False)
        self.set_amt = QtWidgets.QLabel(self.centralwidget)
        self.set_amt.setGeometry(QtCore.QRect(490, 160, 131, 16))
        self.set_amt.setObjectName("set_amt")
        self.line = QtWidgets.QFrame(self.centralwidget)
        self.line.setGeometry(QtCore.QRect(10, 250, 711, 20))
        self.line.setFrameShape(QtWidgets.QFrame.HLine)
        self.line.setFrameShadow(QtWidgets.QFrame.Sunken)
        self.line.setObjectName("line")
        self.update_label = QtWidgets.QLabel(self.centralwidget)
        self.update_label.setGeometry(QtCore.QRect(10, 280, 61, 16))
        self.update_label.setObjectName("update_label")
        self.update_to = QtWidgets.QComboBox(self.centralwidget)
        self.update_to.setGeometry(QtCore.QRect(80, 280, 161, 22))
        self.update_to.setObjectName("update_to")
        self.update_to.addItem("machine_types")
        self.update_to.addItem("task_types")
        self.update_to.addItem("scenario")
        self.update_to.addItem("distribution")
        self.fields_label = QtWidgets.QLabel(self.centralwidget)
        self.fields_label.setGeometry(QtCore.QRect(10, 320, 81, 16))
        self.fields_label.setObjectName("label")
        self.fields = QtWidgets.QLineEdit(self.centralwidget)
        self.fields.setGeometry(QtCore.QRect(100, 320, 501, 20))
        self.fields.setObjectName("fields")
        self.update_submit = QtWidgets.QPushButton(self.centralwidget)
        self.update_submit.setGeometry(QtCore.QRect(610, 320, 75, 23))
        self.update_submit.setObjectName("update_submit")
        self.example_field = QtWidgets.QLineEdit(self.centralwidget)
        self.example_field.setGeometry(QtCore.QRect(100, 350, 501, 20))
        self.example_field.setObjectName("example_field")
        self.example = QtWidgets.QLabel(self.centralwidget)
        self.example.setGeometry(QtCore.QRect(30, 350, 51, 20))
        self.example.setObjectName("example")
        self.example_field.setText("machine_id,machine_name,no_of_replicas,idle_power,max_power,num_of_cores,cpu_clock,memory")
        self.example_field.setDisabled(True)
        self.delete_id_label = QtWidgets.QLabel(self.centralwidget)
        self.delete_id_label.setGeometry(QtCore.QRect(20, 440, 71, 16))
        self.delete_id_label.setObjectName("delete_id_label")
        self.id_to_delete = QtWidgets.QLineEdit(self.centralwidget)
        self.id_to_delete.setGeometry(QtCore.QRect(90, 440, 61, 20))
        self.id_to_delete.setObjectName("id_to_delete")
        self.in_label = QtWidgets.QLabel(self.centralwidget)
        self.in_label.setGeometry(QtCore.QRect(60, 470, 21, 16))
        self.in_label.setObjectName("in_label")
        self.which_to_delete = QtWidgets.QComboBox(self.centralwidget)
        self.which_to_delete.setGeometry(QtCore.QRect(90, 470, 161, 22))
        self.which_to_delete.setObjectName("which_to_delete")
        self.which_to_delete.addItem("")
        self.which_to_delete.addItem("")
        self.which_to_delete.addItem("")
        self.delete_submit = QtWidgets.QPushButton(self.centralwidget)
        self.delete_submit.setGeometry(QtCore.QRect(90, 500, 75, 23))
        self.delete_submit.setObjectName("delete_submit")

        self.wl_label = QtWidgets.QLabel(self.centralwidget)
        self.wl_label.setGeometry(QtCore.QRect(10, 20, 101, 16))
        self.wl_label.setObjectName("wl_label")

        self.tabWidget = QtWidgets.QTabWidget(self.centralwidget)
        self.tabWidget.setGeometry(QtCore.QRect(10, 40, 461, 201))
        self.tabWidget.setObjectName("tabWidget")

        self.wl_generate = QtWidgets.QPushButton(self.centralwidget)
        self.wl_generate.setGeometry(QtCore.QRect(490, 210, 75, 23))
        self.wl_generate.setObjectName("wl_generate")
        self.wl_amount = QtWidgets.QLineEdit(self.centralwidget)
        self.wl_amount.setGeometry(QtCore.QRect(490, 180, 113, 20))
        self.wl_amount.setObjectName("wl_amount")

        MainWindow.setCentralWidget(self.centralwidget)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)
        self.menubar = QtWidgets.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 729, 21))
        self.menubar.setObjectName("menubar")
        self.menuE2C_Sim_Database_Demo = QtWidgets.QMenu(self.menubar)
        self.menuE2C_Sim_Database_Demo.setObjectName("menuE2C_Sim_Database_Demo")
        MainWindow.setMenuBar(self.menubar)
        self.menubar.addAction(self.menuE2C_Sim_Database_Demo.menuAction())

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

        self.scen_load.clicked.connect(self.load_scen)
        self.wl_generate.clicked.connect(self.generate)
        self.update_to.currentTextChanged.connect(self.set_update_to)
        self.update_submit.clicked.connect(self.update_row)
        self.delete_submit.clicked.connect(self.delete)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "MainWindow"))
        self.scen_label.setText(_translate("MainWindow", "Select Scenario(s)"))
        self.scen_load.setText(_translate("MainWindow", "Load"))
        self.fields_label.setText(_translate("MainWindow", "Enter each field:"))
        self.update_label.setText(_translate("MainWindow", "Add row to:"))
        self.wl_label.setText(_translate("MainWindow", "Generated Workload"))
        self.delete_id_label.setText(_translate("MainWindow", "Delete Id of:"))
        self.in_label.setText(_translate("MainWindow", "In:"))
        self.which_to_delete.setItemText(0, _translate("MainWindow", "machine_types"))
        self.which_to_delete.setItemText(1, _translate("MainWindow", "task_types"))
        self.which_to_delete.setItemText(2, _translate("MainWindow", "distribution"))
        self.delete_submit.setText(_translate("MainWindow", "Delete"))
        self.example.setText(_translate("MainWindow", "(Example):"))
        self.wl_generate.setText(_translate("MainWindow", "Generate"))
        self.set_amt.setText(_translate("MainWindow", "Set Amount of Workloads"))
        self.update_submit.setText(_translate("MainWindow", "Submit"))
        self.menuE2C_Sim_Database_Demo.setTitle(_translate("MainWindow", "E2C-Sim Database Demo"))

    def delete(self):
        msgBox = QMessageBox()
        msgBox.setIcon(QMessageBox.Information)
        msgBox.setText("Are you sure you want to delete?")
        msgBox.setWindowTitle("Delete Row")
        msgBox.setStandardButtons(QMessageBox.Ok | QMessageBox.Cancel)
        returnValue = msgBox.exec()

        if returnValue == QMessageBox.Ok:
            self.id = ""
            self.id_to_del = self.id_to_delete.text()
            self.table = self.which_to_delete.currentText()

            if(self.table == "machine_types"):
                self.id = "machine_id"
            if(self.table == "task_types"):
                self.id = "task_id"
            if(self.table == "distribution"):
                self.id = "dist_id"

            cur.execute(f"DELETE FROM {self.table} WHERE {self.id} = {self.id_to_del};")
            conn.commit()

    def update_row(self):
        self.entry = self.fields.text().split(",")
        self.data = []
        
        if(self.update_to.currentText() == "machine_types"):
            self.machine_id = int(self.entry[0])
            self.machine_name = self.entry[1]
            self.no_of_replicas = int(self.entry[2])
            self.idle_power = float(self.entry[3])
            self.max_power = float(self.entry[4])
            self.num_of_cores = int(self.entry[5])
            self.cpu_clock = float(self.entry[6])
            self.memory = float(self.entry[7])

            cur.execute("INSERT INTO machine_types (machine_id,machine_name,no_of_replicas,idle_power,max_power,num_of_cores,cpu_clock,memory)"\
                        f"VALUES({self.machine_id},'{self.machine_name}',{self.no_of_replicas},{self.idle_power},{self.max_power},{self.num_of_cores},{self.cpu_clock},{self.memory})")
            conn.commit()
            # cur.execute("SELECT * FROM machine_types")
            # print(cur.fetchall())

            deleteTables(cur, conn, "eet")
            eet = """ CREATE TABLE IF NOT EXISTS eet (
            task_id INT NOT NULL,
            machine_id INT NOT NULL,
            expected_ex_time FLOAT NOT NULL,

            FOREIGN KEY (task_id) REFERENCES task_types(task_id),
            FOREIGN KEY (machine_id) REFERENCES machine_types(machine_id)
            ); """
            createSchema(cur, conn, eet)
                # --- eet ---
            # This table is derived from task_types and machine_types
            task_type_df = pd.read_sql_query(f'SELECT * FROM task_types', conn)
            machine_type_df = pd.read_sql_query(f'SELECT * FROM machine_types', conn)

            # Cartesian product of task_types and machine_types
            # (only using attributes we need)
            eet_list_buff = [
                (tt, mt) \
                for tt, _, _, _ in task_type_df.values.tolist() \
                for mt, _, _, _, _, _, _, _ in machine_type_df.values.tolist()
            ]

            # Insert randomized eet value for each tuple
            eet_list = []
            for entry in eet_list_buff:
                rnd = round(uniform(MIN_EX, MAX_EX), 2)
                eet_list.append(entry + (rnd, ))

            # Insert data to the eet table
            insertData(cur, conn, eet_list, 'eet')

        if(self.update_to.currentText() == "task_types"):
            self.task_id = int(self.entry[0])
            self.name = self.entry[1]
            self.detail = self.entry[2]
            self.urgency = float(self.entry[3])

            cur.execute(f"INSERT INTO task_types (task_id,name,detail,urgency)"\
            f"VALUES({self.task_id},'{self.name}','{self.detail}',{self.urgency});")
            conn.commit()

            deleteTables(cur, conn, "eet")
            eet = """ CREATE TABLE IF NOT EXISTS eet (
            task_id INT NOT NULL,
            machine_id INT NOT NULL,
            expected_ex_time FLOAT NOT NULL,

            FOREIGN KEY (task_id) REFERENCES task_types(task_id),
            FOREIGN KEY (machine_id) REFERENCES machine_types(machine_id)
            ); """
            createSchema(cur, conn, eet)
                # --- eet ---
            # This table is derived from task_types and machine_types
            task_type_df = pd.read_sql_query(f'SELECT * FROM task_types', conn)
            machine_type_df = pd.read_sql_query(f'SELECT * FROM machine_types', conn)

            # Cartesian product of task_types and machine_types
            # (only using attributes we need)
            eet_list_buff = [
                (tt, mt) \
                for tt, _, _, _ in task_type_df.values.tolist() \
                for mt, _, _, _, _, _, _, _ in machine_type_df.values.tolist()
            ]

            # Insert randomized eet value for each tuple
            eet_list = []
            for entry in eet_list_buff:
                rnd = round(uniform(MIN_EX, MAX_EX), 2)
                eet_list.append(entry + (rnd, ))

            # Insert data to the eet table
            insertData(cur, conn, eet_list, 'eet')

        if(self.update_to.currentText() == "scenario"):
            self.task_id = int(self.entry[0])
            self.start_time = float(self.entry[1])
            self.end_time = float(self.entry[2])
            self.num_of_tasks = int(self.entry[3])
            self.dist_id = int(self.entry[4])

            cur.execute("INSERT INTO scenario (task_id,start_time,end_time,num_of_tasks,dist_id)"\
            f"VALUES({self.task_id},{self.start_time},{self.end_time},{self.num_of_tasks},{self.dist_id});")
            conn.commit()

        if(self.update_to.currentText() == "distribution"):
            self.dist_id = int(self.entry[0])
            self.name = self.entry[1]

            cur.execute("INSERT INTO distribution (dist_id,name)"\
            f"VALUES({self.dist_id},'{self.name}');")
            conn.commit()

    def set_update_to(self):
        if(self.update_to.currentText() == "machine_types"):
            self.example_field.setText("machine_id,machine_name,no_of_replicas,idle_power,max_power,num_of_cores,cpu_clock,memory")
        if(self.update_to.currentText() == "task_types"):
            self.example_field.setText("task_id,name,detail,urgency")
        if(self.update_to.currentText() == "scenario"):
            self.example_field.setText("task_id,start_time,end_time,num_of_tasks,dist_id")
        if(self.update_to.currentText() == "distribution"):
            self.example_field.setText("dist_id,name")
        

    def load_scen(self):
        self.path  = QFileDialog.getOpenFileName(None, caption='Choose Scenario File',
                                    directory=QDir.currentPath(),
                                    filter='*.csv')
        self.scen_path.setText(self.path[0])
        print(f'(main.py) {self.path[0]}')
        scen_values = fromCSV(self.path[0])
        cur.execute("DELETE FROM scenario;")
        insertData(cur, conn, scen_values, 'scenario')
    
    def generate(self):
        self.amount = self.wl_amount.text()
        wl.createWorkload(cur, conn, int(self.amount))

        for k in range(int(self.amount)):
            self.tab = QtWidgets.QWidget()
            self.tab.setObjectName("tab")
            self.tab.layout = QtWidgets.QVBoxLayout()
            self.table = QtWidgets.QTableWidget()
            self.table.setColumnCount(4)
            self.tab.layout.addWidget(self.table)
            self.tab.setLayout(self.tab.layout)

            item = QtWidgets.QTableWidgetItem("id")
            self.table.setHorizontalHeaderItem(0, item)
            item = QtWidgets.QTableWidgetItem("task type")
            self.table.setHorizontalHeaderItem(1, item)
            item = QtWidgets.QTableWidgetItem("task name")
            self.table.setHorizontalHeaderItem(2, item)
            item = QtWidgets.QTableWidgetItem("arrival")
            self.table.setHorizontalHeaderItem(3, item)
                
            cur.execute(f"SELECT * FROM workload{k+1}")
            self.length = len(cur.fetchall())
            self.table.setRowCount(self.length)

            for i in range(self.length):
                cur.execute(f"SELECT task_id FROM workload{k+1} WHERE instance_id = {i+1}")
                self.task_id = cur.fetchone()
                cur.execute(f"SELECT name FROM workload{k+1} WHERE instance_id = {i+1}")
                self.name = cur.fetchone()
                cur.execute(f"SELECT arrival_time FROM workload{k+1} WHERE instance_id = {i+1}")
                self.arrival = cur.fetchone()

                self.table.setItem(i,0, QtWidgets.QTableWidgetItem(str(i+1)))
                self.table.setItem(i,1, QtWidgets.QTableWidgetItem(str(self.task_id)[1]))
                self.table.setItem(i,2, QtWidgets.QTableWidgetItem(str(self.name)[2:4]))
                self.table.setItem(i,3, QtWidgets.QTableWidgetItem(str(self.arrival)[1:-2]))
                
            self.tabWidget.addTab(self.tab, f"WL{k+1}")


if __name__ == "__main__":
    import sys
    initTables(cur, conn)
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())