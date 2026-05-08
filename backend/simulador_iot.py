import random
import math
import time
import json
import threading
from queue import Queue
from datetime import datetime, timedelta


ESTADOS_TIEMPO = ["sol", "nublado", "lluvia", "tormenta", "niebla", "viento"]
ESTADOS_LUNA = ["nueva", "creciente", "cuarto creciente", "llena", "cuarto menguante"]
DIRECCIONES_VIENTO = ["N", "NE", "E", "SE", "S", "SO", "O", "NO"]
CALIDAD_AIRE = ["buena", "moderada", "mala", "muy mala"]


class SimuladorEstacionMeteorologica:
    def __init__(self, intervalo=3):
        # Tiempo entre mediciones simuladas.
        self.intervalo = intervalo

        # Cola limitada: simula un buffer entre sensores y sistema de procesamiento.
        # Esto implementa el patrón productor-consumidor.
        self.cola_datos = Queue(maxsize=10)

        # Lock para proteger datos compartidos entre hilos.
        # Evita condiciones de carrera al acceder a ultimo_dato y datos_generados.
        self.lock = threading.Lock()

        # Event permite detener todos los hilos de forma ordenada.
        self.evento_parada = threading.Event()

        # Última lectura disponible para otros módulos, por ejemplo un WebSocket.
        self.ultimo_dato = None

        # Contador usado para estadísticas de rendimiento.
        self.datos_generados = 0
        self.inicio = time.time()

    def calcular_punto_rocio(self, temperatura, humedad):
        """
        Calcula el punto de rocío usando una aproximación habitual.
        Depende de la temperatura y la humedad relativa.
        """
        a = 17.27
        b = 237.7

        alpha = ((a * temperatura) / (b + temperatura)) + math.log(humedad / 100)
        punto_rocio = (b * alpha) / (a - alpha)

        return round(punto_rocio, 1)

    def generar_pronostico_horas(self):
        """
        Genera un pronóstico horario de 24 horas.
        En una aplicación real estos datos vendrían de una API meteorológica.
        """
        ahora = datetime.now()
        pronostico = []

        for i in range(24):
            hora = ahora + timedelta(hours=i)

            pronostico.append({
                "hora": hora.strftime("%H:00"),
                "temperatura": round(random.uniform(5, 35), 1),
                "humedad": random.randint(30, 95),
                "estado": random.choice(ESTADOS_TIEMPO)
            })

        return pronostico

    def generar_pronostico_semanal(self):
        """
        Genera un pronóstico diario para 7 días.
        Sirve para alimentar el dashboard semanal.
        """
        hoy = datetime.now()
        pronostico = []

        for i in range(7):
            dia = hoy + timedelta(days=i)
            temp_min = round(random.uniform(2, 18), 1)
            temp_max = round(random.uniform(temp_min + 3, 38), 1)

            pronostico.append({
                "dia": dia.strftime("%A"),
                "fecha": dia.strftime("%Y-%m-%d"),
                "temperatura_min": temp_min,
                "temperatura_max": temp_max,
                "humedad": random.randint(30, 95),
                "estado": random.choice(ESTADOS_TIEMPO),
                "precipitacion": round(random.uniform(0, 30), 1)
            })

        return pronostico

    def generar_datos_estacion(self):
        """
        Simula una lectura completa de la estación meteorológica.
        Esta función representa el trabajo que harían varios sensores IoT.
        """
        temperatura = round(random.uniform(5, 35), 1)
        humedad = random.randint(30, 95)

        return {
            "timestamp": datetime.now().isoformat(),

            "temperatura": temperatura,
            "humedad": humedad,
            "punto_rocio": self.calcular_punto_rocio(temperatura, humedad),

            "presion_atmosferica": round(random.uniform(980, 1040), 1),
            "precipitacion": round(random.uniform(0, 20), 1),

            "viento": {
                "posicion": random.choice(DIRECCIONES_VIENTO),
                "km_h": round(random.uniform(0, 80), 1)
            },

            "indice_uv": round(random.uniform(0, 11), 1),
            "visibilidad": round(random.uniform(1, 30), 1),

            "calidad_aire": {
                "estado": random.choice(CALIDAD_AIRE),
                "aqi": random.randint(0, 300)
            },

            "luna": {
                "estado": random.choice(ESTADOS_LUNA),
                "iluminacion": random.randint(0, 100)
            },

            "pronostico_diario_semanal": self.generar_pronostico_semanal(),
            "pronostico_por_horas": self.generar_pronostico_horas()
        }

    def productor(self):
        """
        Hilo productor.

        Su tarea es simular los sensores de la estación.
        Genera una lectura cada cierto intervalo y la mete en la cola.

        Justificación:
        En un sistema IoT real, los sensores producen datos constantemente
        aunque el sistema que los procesa vaya a otra velocidad.
        """
        while not self.evento_parada.is_set():
            datos = self.generar_datos_estacion()

            # La cola comunica el productor con el consumidor.
            # Si la cola está llena, put() espera, evitando perder datos.
            self.cola_datos.put(datos)

            # Sección crítica protegida con Lock.
            # Varios hilos podrían leer o modificar estas variables.
            with self.lock:
                self.ultimo_dato = datos
                self.datos_generados += 1

            time.sleep(self.intervalo)

    def consumidor(self):
        """
        Hilo consumidor.

        Extrae datos de la cola y los procesa.
        Ahora mismo los muestra por pantalla, pero en una versión real
        podría guardarlos en una base de datos, fichero o sistema de logs.

        Justificación:
        Separar generación y procesamiento permite que ambas partes trabajen
        de forma independiente.
        """
        while not self.evento_parada.is_set():
            try:
                datos = self.cola_datos.get(timeout=1)

                print("\n--- NUEVOS DATOS DE LA ESTACIÓN ---")
                print(json.dumps(datos, indent=4, ensure_ascii=False))

                self.cola_datos.task_done()

            except:
                # Si no hay datos en la cola durante 1 segundo,
                # se vuelve a comprobar si hay que parar el hilo.
                pass

    def mostrar_rendimiento(self):
        """
        Hilo de monitorización.

        Calcula cuántas lecturas genera el simulador por segundo.
        Esto permite hacer un análisis básico de rendimiento.
        """
        while not self.evento_parada.is_set():
            time.sleep(10)

            with self.lock:
                tiempo_total = time.time() - self.inicio

                if tiempo_total > 0:
                    rendimiento = self.datos_generados / tiempo_total
                else:
                    rendimiento = 0

                print("\n--- RENDIMIENTO ---")
                print(f"Datos generados: {self.datos_generados}")
                print(f"Tiempo activo: {round(tiempo_total, 2)} segundos")
                print(f"Datos por segundo: {round(rendimiento, 3)}")

    def obtener_ultimo_dato(self):
        """
        Método pensado para ser usado por el futuro servidor WebSocket.

        Devuelve la última lectura generada por los sensores.
        Se usa Lock porque ultimo_dato puede estar siendo actualizado
        al mismo tiempo por el hilo productor.
        """
        with self.lock:
            return self.ultimo_dato

    def iniciar(self):
        """
        Inicia el sistema concurrente completo.

        Se crean tres hilos:
        - productor: genera datos
        - consumidor: procesa datos
        - rendimiento: monitoriza estadísticas
        """
        hilo_productor = threading.Thread(target=self.productor)
        hilo_consumidor = threading.Thread(target=self.consumidor)
        hilo_rendimiento = threading.Thread(target=self.mostrar_rendimiento)

        hilo_productor.start()
        hilo_consumidor.start()
        hilo_rendimiento.start()

        try:
            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            print("\nDeteniendo simulador...")

            # Avisamos a todos los hilos de que deben terminar.
            self.evento_parada.set()

            hilo_productor.join()
            hilo_consumidor.join()
            hilo_rendimiento.join()

            print("Simulador detenido correctamente.")


if __name__ == "__main__":
    simulador = SimuladorEstacionMeteorologica(intervalo=3)
    simulador.iniciar()