import requests
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import time
from typing import List, Dict, Any

class SimemExtractor:
    def __init__(self, base_url: str = "https://www.simem.co/backend-files/api/PublicData"):
        self.base_url = base_url
        self.session = requests.Session()
        
    def get_data(self, date: str, dataset_id: str) -> Dict[str, Any]:
        """
        Obtiene datos de la API para una fecha específica y dataset_id
        """
        params = {
            'startDate': date,
            'endDate': date,  # Siempre iguales según tu observación
            'datasetId': dataset_id
        }
        
        try:
            print(f"🔍 Consultando: {self.base_url}")
            print(f"   Parámetros: {params}")
            
            response = self.session.get(self.base_url, params=params, timeout=30)
            print(f"   Status: {response.status_code}")
            
            response.raise_for_status()
            
            data = response.json()
            print(f"   Success: {data.get('success', 'N/A')}")
            
            if data.get('success'):
                records_count = len(data.get('result', {}).get('records', []))
                print(f"   Records encontrados: {records_count}")
            
            return data
            
        except requests.exceptions.Timeout:
            print(f"❌ Timeout al obtener datos para {date}, dataset {dataset_id}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"❌ Error de conexión para {date}, dataset {dataset_id}: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"❌ Error al decodificar JSON para {date}, dataset {dataset_id}: {e}")
            print(f"   Response text: {response.text[:200]}...")
            return None
        except Exception as e:
            print(f"❌ Error inesperado para {date}, dataset {dataset_id}: {e}")
            return None
    
    def generate_date_range(self, start_date: str, end_date: str, frequency: str = 'daily') -> List[str]:
        """
        Genera un rango de fechas según la frecuencia especificada
        
        Args:
            start_date: Fecha inicio en formato YYYY-MM-DD
            end_date: Fecha fin en formato YYYY-MM-DD  
            frequency: 'daily', 'weekly', 'monthly'
        
        Returns:
            Lista de fechas en formato YYYY-MM-DD (formato esperado por la API SIMEM)
        """
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        dates = []
        
        current = start
        
        while current <= end:
            # La API SIMEM espera fechas en formato YYYY-MM-DD
            dates.append(current.strftime('%Y-%m-%d'))
            
            if frequency == 'daily':
                current += timedelta(days=1)
            elif frequency == 'weekly':
                current += timedelta(weeks=1)
            elif frequency == 'monthly':
                # Avanzar al mismo día del próximo mes
                try:
                    if current.month == 12:
                        current = current.replace(year=current.year + 1, month=1)
                    else:
                        current = current.replace(month=current.month + 1)
                except ValueError:
                    # Manejo de casos como 31 de enero -> 28/29 de febrero
                    if current.month == 12:
                        current = current.replace(year=current.year + 1, month=1, day=1)
                    else:
                        current = current.replace(month=current.month + 1, day=1)
            else:
                raise ValueError("Frecuencia debe ser 'daily', 'weekly' o 'monthly'")
                
        return dates
    
    def save_data(self, data: Dict[str, Any], dataset_id: str, date: str):
        """
        Guarda los records de la respuesta en un archivo JSON
        """
        if not data:
            print(f"❌ No hay datos para guardar en {date}")
            return
            
        # La estructura correcta es data['result']['records']
        if 'result' not in data or 'records' not in data['result']:
            print(f"❌ Estructura de datos incorrecta para {date}")
            print(f"   Claves disponibles: {list(data.keys())}")
            return
            
        records = data['result']['records']
        if not records:
            print(f"⚠️  Lista de records vacía para {date}")
            return
            
        # Crear directorio
        directory = Path(f"data/simem/{dataset_id}")
        directory.mkdir(parents=True, exist_ok=True)
        
        # Nombre del archivo
        filename = f"{date}.json"
        filepath = directory / filename
        
        # Guardar solo los records
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
            
        print(f"✅ Guardado: {filepath} ({len(records)} records)")
    
    def extract_data_range(self, 
                          start_date: str, 
                          end_date: str, 
                          dataset_ids: List[str], 
                          frequency: str = 'daily',
                          delay: float = 1.0):
        """
        Extrae datos para un rango de fechas y múltiples dataset_ids
        
        Args:
            start_date: Fecha inicio (YYYY-MM-DD)
            end_date: Fecha fin (YYYY-MM-DD)
            dataset_ids: Lista de dataset IDs
            frequency: Frecuencia de extracción
            delay: Pausa entre requests en segundos
        """
        dates = self.generate_date_range(start_date, end_date, frequency)
        total_requests = len(dates) * len(dataset_ids)
        current_request = 0
        
        print(f"Iniciando extracción...")
        print(f"Fechas: {len(dates)} ({frequency})")
        print(f"Dataset IDs: {len(dataset_ids)}")
        print(f"Total requests: {total_requests}")
        print("-" * 50)
        
        for dataset_id in dataset_ids:
            print(f"\nProcesando dataset: {dataset_id}")
            
            for date in dates:
                current_request += 1
                print(f"[{current_request}/{total_requests}] Obteniendo {date}...")
                
                data = self.get_data(date, dataset_id)
                if data:
                    self.save_data(data, dataset_id, date)
                
                # Pausa para no sobrecargar la API
                if delay > 0:
                    time.sleep(delay)
        
        print(f"\n✅ Extracción completada!")

def main():
    """
    Función principal - configura aquí tus parámetros
    """
    extractor = SimemExtractor()
    
    # Configuración
    START_DATE = "2024-01-01"
    END_DATE = "2024-01-31"  # Cambia según necesites
    DATASET_IDS = ["670221"]  # Agrega más IDs si necesitas
    FREQUENCY = "daily"  # 'daily', 'weekly', 'monthly'
    DELAY = 1.0  # Segundos entre requests
    
    # Ejecutar extracción
    extractor.extract_data_range(
        start_date=START_DATE,
        end_date=END_DATE,
        dataset_ids=DATASET_IDS,
        frequency=FREQUENCY,
        delay=DELAY
    )

if __name__ == "__main__":
    # Ejemplo de uso directo
    extractor = SimemExtractor()
    
    # # Para un solo día y dataset (ejemplo de prueba)
    # print("🧪 Probando con un solo día...")
    # data = extractor.get_data("2025-07-19", "670221")  # Fecha que sabemos que funciona
    # if data:
    #     extractor.save_data(data, "670221", "2025-07-19")
    #     print("✅ Prueba exitosa")
    # else:
    #     print("❌ Error en la prueba")
    #     print("Verifica tu conexión a internet y que la API esté disponible")
    
    # print("\n" + "="*50)
    
    # Para un rango de fechas (descomenta para usar)
    print("📊 Iniciando extracción de rango...")
    extractor.extract_data_range(
        start_date="2024-01-01",  # Fechas más recientes
        end_date="2024-12-31",
        dataset_ids=[
            "E17D25",
            "972263",
            "0bfc9d",
            "d31647"
        ],
        frequency="daily",  # Cambiar a 'daily', 'weekly' o 'monthly' según necesites
        delay=1.0  # Aumenté la pausa para ser más conservador
    )