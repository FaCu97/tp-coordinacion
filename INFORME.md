Identificación de Clientes (Gateway):
Al establecerse la conexión, el Message Handler del Gateway le asigna un UUID a cada cliente. Esto permite identificar los mensajes en el sistema y que los nodos mantengan el estado de múltiples clientes de forma concurrente sin mezclar sus datos.

Sincronizacion del EOF del cliente:
Dado que la comunicación entre Gateway y Sum se da mediante una Work Queue, el mensaje EOF es consumido por un único Sum. Para notificar al resto, dicha réplica retransmite el EOF a través de un exchange de control a todos los sums.

Para evitar condiciones de carrera es necesario que el QoS esté configurado con prefetch=1, así cuando Sum procesa el EOF, en la cola no hay mensajes pendientes de ese cliente.
La precondicion necesaria es que el mensaje en procesamiento (que puede ser del mismo cliente del EOF), no falle ni vuelva al middleware.

Sharding y Barrera de Sincronización:
Al finalizar su procesamiento, cada réplica de Sum realiza un sharding hasheando el nombre de la fruta para enviar los recuentos parciales de cada una a un único nodo Aggregation.
Para saber cuándo terminar, cada Aggregation implementa una barrera de sincronización mediante un diccionario contador: espera recibir exactamente N mensajes de EOF por cada cliente (donde N es la cantidad de réplicas de Sum). Solo cuando todos los workers han confirmado su finalización, el Aggregator calcula y emite el Top de frutas resultante del cliente hacia el Join.

Fusión de resultados:
Finalmente, Join recibe los tops por parte de los Aggregation y cuando recibió los tops de todos los Aggregation para algún cliente, emite el top global.