import requests
import os
import csv
import time
from functools import wraps
from typing import Iterator, List, Tuple, Dict, Any, Union, Generator, Callable, TypeVar

F = TypeVar('F', bound=Callable[..., Any])

#DEKORATOR CZASU
def timer(func: F) -> F:
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        print(f"\nSTART: Wywołanie metody '{func.__name__}'")
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"KONIEC: Metoda '{func.__name__}' zakończyła działanie.")
        print(f"CZAS WYKONANIA: {execution_time:.4f} s")
        return result
    return wrapper

#POBIERANIE PLIKÓW + WYJĄTKI
class FileDownloadError(Exception):
    """Główna klasa bazowa dla niestandardowych wyjątków pobierania plików."""
    pass
class NotFoundError(FileDownloadError):
    """Wyjątek rzucany dla kodu HTTP 404 (Nie znaleziono)."""
    pass

class AccessDeniedError(FileDownloadError):
    """Wyjątek rzucany dla kodu HTTP 403 (Odmowa dostępu)."""
    pass

FILE_URL = "https://oleksandr-fedoruk.com/wp-content/uploads/2025/10/sample.csv"

def download_file(url: str, filename: str = 'latest.csv') -> None:
    print(f"\nPobieranie pliku z: {url}")
    print(f"Zapisywanie jako: {filename}")
    try:
        response = requests.get(url, stream=True)

        if response.status_code == 404:
            raise NotFoundError(f"Błąd 404: Nie znaleziono pliku pod adresem URL: {url}")
        
        if response.status_code == 403:
            raise AccessDeniedError(f"Błąd 403: Odmowa dostępu do pliku pod adresem URL: {url}")

        response.raise_for_status()

        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk: 
                    f.write(chunk)
        
        print(f"Zapisano plik: {filename} (Rozmiar: {os.path.getsize(filename)} bajtów)")

    except NotFoundError as e:
        print(f"Plik nie istnieje: {e}")
    except AccessDeniedError as e:
        print(f"Brak uprawnień: {e}")
    
    except requests.exceptions.HTTPError as e:
        print(f"Wystąpił błąd serwera (np. 401, 500): {e}")
    except requests.exceptions.RequestException as e:
        print(f"Wystąpił błąd połączenia: {e}")
    except Exception as e:
        print(f"Wystąpił nieoczekiwany błąd: {e}")

if __name__ == "__main__":
    print("--- TEST 1: Prawidłowe pobranie ---")
    download_file(FILE_URL)

    #symulacja błędu 404
    print("\n--- TEST 2: Symulacja błędu 404 (NotFoundError) ---")
    BROKEN_URL_404 = "https://oleksandr-fedoruk.com/wp-content/uploads/2025/10/nonexistent.csv"
    download_file(BROKEN_URL_404, "404_test.csv")
    
    #symulacja błędu 403
    print("\n--- TEST 3: Symulacja błędu 403 (AccessDeniedError) ---")
    BLOCKED_URL_403 = "https://httpbin.org/status/403" 
    download_file(BLOCKED_URL_403, "403_test.csv")

#ETL
class ETLProcessor:
    def __init__(self, input_filename: str):
        self._input_filename = input_filename
    
    @property
    def input_filename(self) -> str:
        print(f"Odczytano nazwę pliku: {self._input_filename}")
        return self._input_filename

    @input_filename.setter
    def input_filename(self, new_filename: str):
        if not isinstance(new_filename, str) or not new_filename.strip():
            raise ValueError("Nazwa pliku musi być niepustym ciągiem znaków.")
        print(f"Zmieniono plik wejściowy z '{self._input_filename}' na '{new_filename}'")
        self._input_filename = new_filename

    @input_filename.deleter
    def input_filename(self):
        print("Usunięto nazwę pliku wejściowego.")
        self._input_filename = None
        
    @property
    def report_name(self) -> str:
        base_name = os.path.basename(self._input_filename or 'BRAK_PLIKU')
        return f"RAPORT_DANYCH_Z_{base_name.upper()}"

    @staticmethod
    def format_missing_indices(indices: List[int]) -> str:
        return ','.join(map(str, indices))

    def extract_lines(self) -> Generator[List[str], None, None]:
        filename_to_use = self._input_filename
        if not filename_to_use:
             print(f"Nie można uruchomić Extract: Brak zdefiniowanej nazwy pliku.")
             return  
        try:
            with open(filename_to_use, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                next(reader, None)
                for row in reader:
                    yield row
        except FileNotFoundError:
            print(f"Plik źródłowy '{filename_to_use}' nie istnieje.")
            return

    def transform_data(self) -> Generator[Tuple[Dict[str, Union[int, float]], Dict[str, Union[int, List[int]]]], None, None]:
        for row in self.extract_lines():
            if not row or len(row) < 8: 
                continue
            try:
                order_number = int(row[0])
            except ValueError:
                continue
            data_values = row[1:8]
            numeric_values = [float(x) for x in data_values if x != '-']
            current_sum = sum(numeric_values)
            current_count = len(numeric_values)
            current_mean = current_sum / current_count if current_count else 0.0
            values_data = {
                'numer_porzadkowy': order_number,
                'suma': round(current_sum, 2),
                'srednia': round(current_mean, 2)
            }
            missing_indices = [
                i + 1 
                for i, value in enumerate(data_values) 
                if value == '-'
            ]
            missing_data = {
                'numer_porzadkowy': order_number,
                'indeksy_brakujacych_kolumn': missing_indices
            }
            yield values_data, missing_data
    
    @timer
    def load_data(self, values_output: str, missing_output: str):
        values_fieldnames = ['numer_porzadkowy', 'suma', 'srednia']
        missing_fieldnames = ['numer_porzadkowy', 'indeksy_kolumn_z_myslnikami']
        try:
            with open(values_output, 'w', newline='', encoding='utf-8') as f_values, \
                 open(missing_output, 'w', newline='', encoding='utf-8') as f_missing:
                writer_values = csv.DictWriter(f_values, fieldnames=values_fieldnames)
                writer_missing = csv.DictWriter(f_missing, fieldnames=missing_fieldnames)
                writer_values.writeheader()
                writer_missing.writeheader()
                print(f"Rozpoczęcie generowania {self.report_name}") 
                for values_data, missing_data in self.transform_data():
                    writer_values.writerow(values_data)
                    formatted_indices = self.format_missing_indices(missing_data['indeksy_brakujacych_kolumn'])
                    missing_data_to_write = {
                        'numer_porzadkowy': missing_data['numer_porzadkowy'],
                        'indeksy_kolumn_z_myslnikami': formatted_indices
                    }
                    writer_missing.writerow(missing_data_to_write)
            print(f"Dwa pliki CSV utworzone: {values_output}, {missing_output}")
        except Exception as e:
            print(f"Wystąpił błąd podczas zapisu plików: {e}")


FILE_URL = "https://oleksandr-fedoruk.com/wp-content/uploads/2025/10/sample.csv"
INPUT_FILENAME = "dane_zrodlowe.csv"
OUTPUT_VALUES = "values.csv"
OUTPUT_MISSING = "missing_values.csv"

if __name__ == "__main__":
    print("POBIERANIE PLIKU ŹRÓDŁOWEGO ---")
    try:
        download_file(FILE_URL, INPUT_FILENAME)
        if os.path.exists(INPUT_FILENAME):
            print("\nPRZETWARZANIE DANYCH (ETL) ---")
            processor = ETLProcessor(INPUT_FILENAME)
            current_file = processor.input_filename
            print(f"Bieżący plik: {current_file}")
            processor.input_filename = "nowe_dane.csv"
            print(f"Nowa nazwa pliku: {processor.input_filename}")
            processor.input_filename = INPUT_FILENAME 
            processor.load_data(OUTPUT_VALUES, OUTPUT_MISSING)
            print("\n--- PROCES ZAKOŃCZONY POMYŚLNIE ---")
        else:
            print("Nie można przetwarzać: Plik źródłowy nie istnieje.")     
    except FileDownloadError:
        print("\nBłąd krytyczny podczas pobierania. Proces ETL nie został uruchomiony.")
    except Exception as e:
        print(f"\nWystąpił nieoczekiwany błąd: {e}")

