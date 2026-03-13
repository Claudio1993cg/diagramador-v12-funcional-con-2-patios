"""Crea datos_salidas.xlsx mínimo para pruebas (mismo formato que carga el loader)."""
from openpyxl import Workbook

RUTA = "datos_salidas.xlsx"
# Encabezados esperados por loaders.cargar_salidas_desde_excel
HEADERS = ["Linea", "Sentido", "Origen", "Destino", "Hora Inicio", "Hora Fin", "Kilometros"]
# Viajes mínimos: nodos coherentes con config (Deposito Pie Andino, PIE ANDINO, etc.)
VIAJES = [
    ("L1", "Ida", "Deposito Pie Andino", "PIE ANDINO", "06:00", "06:45", 12.5),
    ("L1", "Vuelta", "PIE ANDINO", "Deposito Pie Andino", "07:00", "07:50", 12.5),
    ("L2", "Ida", "Deposito Pie Andino", "INTERMODAL", "06:30", "07:30", 20.0),
    ("L2", "Vuelta", "INTERMODAL", "Deposito Pie Andino", "08:00", "09:00", 20.0),
    ("L3", "Ida", "Deposito Pie Andino", "LOS TILOS", "07:00", "07:40", 15.0),
    ("L3", "Vuelta", "LOS TILOS", "Deposito Pie Andino", "08:00", "08:45", 15.0),
]

def main():
    wb = Workbook()
    ws = wb.active
    ws.title = "Salidas"
    ws.append(HEADERS)
    for v in VIAJES:
        ws.append(list(v))
    wb.save(RUTA)
    print(f"Creado {RUTA} con {len(VIAJES)} viajes.")

if __name__ == "__main__":
    main()
