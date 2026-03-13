# Ejecutar la aplicacion web del diagramador (Flask, puerto 5000)
# Uso: .\ejecutar_web.ps1
$raiz = $PSScriptRoot
Set-Location $raiz
# Asegurar que el proyecto este en PYTHONPATH para imports de diagramador_optimizado y web
$env:PYTHONPATH = $raiz
Write-Host "Iniciando servidor web en http://127.0.0.1:5000"
Write-Host "Abre el navegador en esa direccion para usar el diagramador."
Write-Host ""
python app.py
