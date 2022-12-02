from Utilities.utilities import *
from initTables import *
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtWidgets import QFileDialog
from PyQt5.QtCore import QDir
from Utilities.utilities import createSchema, fromCSV, insertData, printTable, deleteTables
import workload as wl
from initTables import initTables
import sqlite3 as sq

# GUI 

# DONE - 1. Provide user ability to load scenario file

# DONE - 2. Use file path as input to fromCSV

# DONE - 3. Use data fromCSV to insertData(scenario)

# GUI user input for num_wl

# Generate trigger button calls createWorkload(num_wls)

# 4. Display workload tables to user
    # 5 wls --> workload1, workload2, ..., workload5
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
        self.scen_label.setGeometry(QtCore.QRect(10, 10, 91, 16))
        self.scen_label.setObjectName("scen_label")
        self.scen_load = QtWidgets.QPushButton(self.centralwidget)
        self.scen_load.setGeometry(QtCore.QRect(20, 80, 75, 23))
        self.scen_load.setObjectName("scen_load")
        self.scen_path = QtWidgets.QLineEdit(self.centralwidget)
        self.scen_path.setGeometry(QtCore.QRect(20, 50, 600, 20))
        self.scen_path.setObjectName("scen_path")
        self.scen_path.setText("(path to scenario here)")
        self.scen_path.setEnabled(False)
        
        self.wl_label = QtWidgets.QLabel(self.centralwidget)
        self.wl_label.setGeometry(QtCore.QRect(10, 280, 101, 16))
        self.wl_label.setObjectName("wl_label")
        self.wl_table = QtWidgets.QTableWidget(self.centralwidget)
        self.wl_table.setGeometry(QtCore.QRect(10, 310, 511, 192))
        self.wl_table.setObjectName("wl_table")
        self.wl_table.setColumnCount(4)
        
        item = QtWidgets.QTableWidgetItem()
        self.wl_table.setHorizontalHeaderItem(0, item)
        item = QtWidgets.QTableWidgetItem()
        self.wl_table.setHorizontalHeaderItem(1, item)
        item = QtWidgets.QTableWidgetItem()
        self.wl_table.setHorizontalHeaderItem(2, item)
        item = QtWidgets.QTableWidgetItem()
        self.wl_table.setHorizontalHeaderItem(3, item)
        self.wl_generate = QtWidgets.QPushButton(self.centralwidget)
        self.wl_generate.setGeometry(QtCore.QRect(10, 510, 75, 23))
        self.wl_generate.setObjectName("wl_generate")
        self.wl_amount = QtWidgets.QLineEdit(self.centralwidget)
        self.wl_amount.setGeometry(QtCore.QRect(100, 510, 113, 20))
        self.wl_amount.setObjectName("wl_amount")

        MainWindow.setCentralWidget(self.centralwidget)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)
        self.menubar = QtWidgets.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 796, 21))
        self.menubar.setObjectName("menubar")
        self.menuE2C_Sim_Database_Demo = QtWidgets.QMenu(self.menubar)
        self.menuE2C_Sim_Database_Demo.setObjectName("menuE2C_Sim_Database_Demo")
        MainWindow.setMenuBar(self.menubar)
        self.menubar.addAction(self.menuE2C_Sim_Database_Demo.menuAction())

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

        self.scen_load.clicked.connect(self.load_scen)
        self.wl_generate.clicked.connect(self.generate)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "MainWindow"))
        self.scen_label.setText(_translate("MainWindow", "Select Scenario(s)"))
        self.scen_load.setText(_translate("MainWindow", "Load"))
        self.wl_label.setText(_translate("MainWindow", "Generated Workload"))
        self.wl_generate.setText(_translate("MainWindow", "Generate"))
        item = self.wl_table.horizontalHeaderItem(0)
        item.setText(_translate("MainWindow", "id"))
        item = self.wl_table.horizontalHeaderItem(1)
        item.setText(_translate("MainWindow", "task type"))
        item = self.wl_table.horizontalHeaderItem(2)
        item.setText(_translate("MainWindow", "name"))
        item = self.wl_table.horizontalHeaderItem(3)
        item.setText(_translate("MainWindow", "arrival"))
        self.menuE2C_Sim_Database_Demo.setTitle(_translate("MainWindow", "E2C-Sim Database Demo"))

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

        cur.execute(f"SELECT * FROM workload{self.amount}")
        self.length = len(cur.fetchall())
        printTable(cur, f'workload{self.amount}')
        self.wl_table.setRowCount(self.length)

        for i in range(self.length):
            cur.execute(f"SELECT task_id FROM workload{self.amount} WHERE instance_id = {i+1}")
            self.task_id = cur.fetchone()
            cur.execute(f"SELECT name FROM workload{self.amount} WHERE instance_id = {i+1}")
            self.name = cur.fetchone()
            cur.execute(f"SELECT arrival_time FROM workload{self.amount} WHERE instance_id = {i+1}")
            self.arrival = cur.fetchone()

            self.wl_table.setItem(i,0, QtWidgets.QTableWidgetItem(str(i+1)))
            self.wl_table.setItem(i,1, QtWidgets.QTableWidgetItem(str(self.task_id)[1]))
            self.wl_table.setItem(i,2, QtWidgets.QTableWidgetItem(str(self.name)[2:4]))
            self.wl_table.setItem(i,3, QtWidgets.QTableWidgetItem(str(self.arrival)[1:-2]))


if __name__ == "__main__":
    import sys
    initTables(cur, conn)
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())