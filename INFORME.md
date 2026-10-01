Redactar un breve informe en el archivo `INFORME.md` explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes, grándes volúmens de datos y la cantidad de controles.

El message handler del gateway, le asigna un uuid a cada cliente, para identificar luego los mensajes que transmite.

Sincronizacion del EOF del cliente:
El cliente envia el EOF y lo recibe un Sum, ese Sum lo comunica por un exchange a todos los sums.
Para evitar condiciones de carrera es necesario que el QoS esté configurado con prefetch=1, así cuando Sum procesa el EOF, en la cola no hay mensajes pendientes de ese cliente.
La precondicion necesaria es que el mensaje en procesamiento (que puede ser del mismo cliente del EOF), no falle ni vuelva al middleware.

Al finalizar su procesamiento, cada réplica de Sum envía su propio mensaje de EOF hacia la siguiente etapa. El nodo Aggregation implementa una barrera de sincronización mediante un diccionario contador: espera recibir exactamente N mensajes de EOF por cada cliente (donde N es la cantidad de réplicas de Sum). Solo cuando todos los workers han confirmado su finalización, el Aggregator calcula y emite el Top de frutas resultante.