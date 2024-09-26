import sys
import pandas as pd
import requests
import json
import logging
from PyQt6.QtWidgets import QApplication, QMainWindow, QPushButton, QFileDialog, QProgressBar, QVBoxLayout, QWidget, QLabel, QCheckBox, QScrollArea
from PyQt6.QtCore import QThread, pyqtSignal
from datetime import datetime, timedelta

# Configuração do logging
logging.basicConfig(filename='cnpj_processor.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

class CNPJCache:
    def __init__(self, cache_file='cnpj_cache.json'):
        self.cache_file = cache_file
        self.cache = self.load_cache()

    def load_cache(self):
        try:
            with open(self.cache_file, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_cache(self):
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f)

    def get(self, cnpj):
        if cnpj in self.cache:
            data, timestamp = self.cache[cnpj]
            if datetime.now() - datetime.fromisoformat(timestamp) < timedelta(days=30):
                return data
        return None

    def set(self, cnpj, data):
        self.cache[cnpj] = (data, datetime.now().isoformat())
        self.save_cache()

class CNPJProcessor(QThread):
    progress_updated = pyqtSignal(int)
    process_finished = pyqtSignal()
    error_occurred = pyqtSignal(str)

    def __init__(self, file_path, fields_to_update):
        super().__init__()
        self.file_path = file_path
        self.fields_to_update = fields_to_update
        self.cache = CNPJCache()

    def run(self):
        try:
            df = pd.read_excel(self.file_path)
            total_rows = len(df)

            for index, row in df.iterrows():
                try:
                    cnpj = str(row['CNPJ']).zfill(14)
                    data = self.cache.get(cnpj)
                    if not data:
                        data = self.fetch_cnpj_data(cnpj)
                        if data:
                            self.cache.set(cnpj, data)

                    if data:
                        for key in self.fields_to_update:
                            if key in data:
                                df.at[index, key] = data[key]
                        logging.info(f"CNPJ {cnpj} atualizado com sucesso.")
                    else:
                        logging.warning(f"Não foi possível obter dados para o CNPJ {cnpj}.")

                except Exception as e:
                    logging.error(f"Erro ao processar CNPJ {cnpj}: {str(e)}")

                progress = int((index + 1) / total_rows * 100)
                self.progress_updated.emit(progress)

            output_file = self.file_path.replace('.xlsx', '_atualizado.xlsx')
            df.to_excel(output_file, index=False)
            logging.info(f"Processamento concluído. Arquivo salvo: {output_file}")
            self.process_finished.emit()

        except Exception as e:
            error_msg = f"Erro durante o processamento: {str(e)}"
            logging.error(error_msg)
            self.error_occurred.emit(error_msg)

    def fetch_cnpj_data(self, cnpj):
        url = f"https://www.receitaws.com.br/v1/cnpj/{cnpj}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Erro na requisição para CNPJ {cnpj}: {str(e)}")
            return None

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Analisador e Saneador de CNPJ")
        self.setGeometry(100, 100, 500, 400)

        layout = QVBoxLayout()

        self.file_button = QPushButton("Selecionar Arquivo Excel")
        self.file_button.clicked.connect(self.select_file)
        layout.addWidget(self.file_button)

        self.status_label = QLabel("Nenhum arquivo selecionado")
        layout.addWidget(self.status_label)

        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_content = QWidget()
        self.scroll_layout = QVBoxLayout(self.scroll_content)
        self.scroll_area.setWidget(self.scroll_content)
        layout.addWidget(self.scroll_area)

        self.progress_bar = QProgressBar()
        layout.addWidget(self.progress_bar)

        self.process_button = QPushButton("Processar")
        self.process_button.clicked.connect(self.start_processing)
        self.process_button.setEnabled(False)
        layout.addWidget(self.process_button)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

        self.file_path = None
        self.field_checkboxes = []

    def select_file(self):
        self.file_path, _ = QFileDialog.getOpenFileName(self, "Selecionar arquivo Excel", "", "Excel Files (*.xlsx)")
        if self.file_path:
            self.status_label.setText(f"Arquivo selecionado: {self.file_path}")
            self.load_fields()

    def load_fields(self):
        try:
            df = pd.read_excel(self.file_path)
            fields = df.columns.tolist()

            for checkbox in self.field_checkboxes:
                self.scroll_layout.removeWidget(checkbox)
                checkbox.deleteLater()
            self.field_checkboxes.clear()

            for field in fields:
                checkbox = QCheckBox(field)
                self.field_checkboxes.append(checkbox)
                self.scroll_layout.addWidget(checkbox)

            self.process_button.setEnabled(True)
        except Exception as e:
            self.status_label.setText(f"Erro ao carregar arquivo: {str(e)}")
            logging.error(f"Erro ao carregar arquivo: {str(e)}")

    def start_processing(self):
        selected_fields = [cb.text() for cb in self.field_checkboxes if cb.isChecked()]
        if not selected_fields:
            self.status_label.setText("Por favor, selecione pelo menos um campo para atualizar.")
            return

        self.processor = CNPJProcessor(self.file_path, selected_fields)
        self.processor.progress_updated.connect(self.update_progress)
        self.processor.process_finished.connect(self.process_completed)
        self.processor.error_occurred.connect(self.show_error)
        self.processor.start()

        self.process_button.setEnabled(False)
        self.file_button.setEnabled(False)

    def update_progress(self, value):
        self.progress_bar.setValue(value)

    def process_completed(self):
        self.status_label.setText("Processamento concluído!")
        self.process_button.setEnabled(True)
        self.file_button.setEnabled(True)

    def show_error(self, error_msg):
        self.status_label.setText(f"Erro: {error_msg}")
        self.process_button.setEnabled(True)
        self.file_button.setEnabled(True)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())