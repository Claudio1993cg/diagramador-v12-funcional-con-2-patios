# Ejecutar diagramador con PYTHONPATH correcto (raíz del proyecto)
# Uso: .\ejecutar_diagramador.ps1
$raiz = $PSScriptRoot
$env:PYTHONPATH = $raiz
Set-Location $raiz
python diagramador_optimizado/cli/main.py
Read-Host "Presiona Enter para cerrar"
