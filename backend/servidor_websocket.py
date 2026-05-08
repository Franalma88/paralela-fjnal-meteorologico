import asyncio
import json
import threading

from websockets.asyncio.server import serve
from simulador_iot import SimuladorEstacionMeteorologica


# Clientes web conectados actualmente.
# Es un conjunto compartido por varias corrutinas, por eso se controla su uso.
clientes_conectados = set()


async def enviar_datos_cliente(websocket, simulador):
    """
    Atiende a un cliente WebSocket.

    Cada navegador conectado ejecutará esta corrutina.
    El servidor no genera los datos aquí: los recoge del simulador IoT.
    """

    clientes_conectados.add(websocket)
    print(f"Cliente conectado. Total clientes: {len(clientes_conectados)}")

    try:
        while True:
            # Obtenemos la última lectura generada por el simulador.
            # Este método ya usa Lock internamente en simulador_iot.py.
            dato = simulador.obtener_ultimo_dato()

            if dato is not None:
                mensaje = json.dumps(dato, ensure_ascii=False)
                await websocket.send(mensaje)

            # Frecuencia de envío al cliente.
            await asyncio.sleep(2)

    except Exception as error:
        print("Cliente desconectado:", error)

    finally:
        clientes_conectados.remove(websocket)
        print(f"Cliente eliminado. Total clientes: {len(clientes_conectados)}")


async def main():
    """
    Inicia el simulador IoT y el servidor WebSocket.

    Aquí conviven dos modelos concurrentes:

    1. threading:
       El simulador usa hilos para producir y consumir datos.

    2. asyncio:
       El servidor WebSocket atiende clientes de forma asíncrona.

    Esto está justificado porque:
    - La simulación de sensores funciona de manera continua en segundo plano.
    - El servidor debe poder atender varios clientes web a la vez.
    """

    simulador = SimuladorEstacionMeteorologica(intervalo=3)

    # Iniciamos solo los hilos necesarios del simulador.
    # No usamos simulador.iniciar() porque ese método bloquea el programa.
    hilo_productor = threading.Thread(
        target=simulador.productor,
        daemon=True
    )

    hilo_consumidor = threading.Thread(
        target=simulador.consumidor,
        daemon=True
    )

    hilo_rendimiento = threading.Thread(
        target=simulador.mostrar_rendimiento,
        daemon=True
    )

    hilo_productor.start()
    hilo_consumidor.start()
    hilo_rendimiento.start()

    print("Simulador IoT iniciado.")
    print("Servidor WebSocket disponible en ws://localhost:8765")

    async def handler(websocket):
        await enviar_datos_cliente(websocket, simulador)

    async with serve(handler, "localhost", 8765):
   # async with serve(handler, "0.0.0.0", 8765):
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nServidor detenido manualmente.")