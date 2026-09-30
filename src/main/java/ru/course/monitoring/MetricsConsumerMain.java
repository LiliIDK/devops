package ru.course.monitoring;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

public class MetricsConsumerMain {

    public static void main(String[] args) throws Exception {

        /*
         * Получаем настройки подключения из environment variables.
         *
         * Эти значения передаёт docker-compose.yml.
         */


        /*
         * Имя RabbitMQ внутри Docker network.
         */
        String rabbitHost =
                getEnv(
                        "RABBITMQ_HOST",
                        "rabbitmq"
                );


        /*
         * AMQP порт RabbitMQ.
         */
        int rabbitPort =
                Integer.parseInt(
                        getEnv(
                                "RABBITMQ_PORT",
                                "5672"
                        )
                );


        /*
         * Пользователь RabbitMQ.
         */
        String rabbitUser =
                getEnv(
                        "RABBITMQ_USER",
                        "monitoring"
                );


        /*
         * Пароль RabbitMQ.
         */
        String rabbitPassword =
                getEnv(
                        "RABBITMQ_PASSWORD",
                        "monitoring123"
                );


        /*
         * Очередь, которую будем слушать.
         *
         * Именно в эту же очередь агенты публикуют метрики.
         */
        String queueName =
                getEnv(
                        "RABBITMQ_QUEUE",
                        "network.metrics"
                );


        /*
         * Адрес VictoriaMetrics.
         *
         * Consumer будет получать сообщение из RabbitMQ,
         * а потом отправлять его сюда.
         */
        String victoriaUrl =
                getEnv(
                        "VICTORIA_URL",
                        "http://victoriametrics:8428/api/v1/import/prometheus"
                );


        System.out.println("Metrics Consumer started");

        System.out.println(
                "RabbitMQ: "
                        + rabbitHost
                        + ":"
                        + rabbitPort
        );

        System.out.println(
                "Queue: "
                        + queueName
        );

        System.out.println(
                "VictoriaMetrics: "
                        + victoriaUrl
        );


        /*
         * Создаём объект с настройками подключения RabbitMQ.
         */
        ConnectionFactory factory =
                new ConnectionFactory();

        factory.setHost(rabbitHost);
        factory.setPort(rabbitPort);
        factory.setUsername(rabbitUser);
        factory.setPassword(rabbitPassword);


        /*
         * Если соединение пропало,
         * библиотека попробует восстановить его автоматически.
         */
        factory.setAutomaticRecoveryEnabled(true);

        factory.setTopologyRecoveryEnabled(true);


        /*
         * ВОТ ЗДЕСЬ consumer реально подключается к RabbitMQ.
         *
         * metrics-consumer ---> rabbitmq:5672
         */
        Connection connection =
                factory.newConnection(
                        "metrics-consumer"
                );


        /*
         * Создаём Channel внутри Connection.
         *
         * Через этот Channel будем:
         *
         * - слушать очередь;
         * - подтверждать сообщения;
         * - возвращать сообщения обратно в очередь.
         */
        Channel channel =
                connection.createChannel();


        /*
         * Убеждаемся, что очередь существует.
         *
         * Параметры должны совпадать
         * с параметрами очереди у publisher.
         */
        channel.queueDeclare(

                queueName,

                // durable
                true,

                // exclusive
                false,

                // autoDelete
                false,

                null
        );


        /*
         * basicQos(10) означает:
         *
         * RabbitMQ может одновременно передать этому consumer
         * максимум 10 сообщений,
         * которые ещё не были ACK-нуты.
         *
         * Это защищает consumer от ситуации,
         * когда RabbitMQ завалит его тысячами сообщений сразу.
         */
        channel.basicQos(10);


        /*
         * Создаём клиент VictoriaMetrics.
         *
         * Он нужен для последующей отправки сообщения:
         *
         * RabbitMQ
         *    ↓
         * Consumer
         *    ↓
         * VictoriaMetrics
         */
        VictoriaMetricsClient victoriaClient =
                new VictoriaMetricsClient(
                        victoriaUrl
                );


        /*
         * DeliverCallback — это функция,
         * которую RabbitMQ будет вызывать
         * КАЖДЫЙ РАЗ,
         * когда consumer получает новое сообщение.
         *
         * Можно мысленно читать так:
         *
         * "Когда пришло сообщение → выполнить этот код".
         */
        DeliverCallback deliverCallback =
                (consumerTag, delivery) -> {

                    /*
                     * consumerTag — служебный идентификатор
                     * данного consumer.
                     *
                     * В нашей бизнес-логике он пока не нужен.
                     */


                    /*
                     * delivery — ЭТО ОДНО КОНКРЕТНОЕ СООБЩЕНИЕ,
                     * которое RabbitMQ только что передал consumer.
                     *
                     * В delivery находится:
                     *
                     * - body сообщения;
                     * - routing key;
                     * - информация о доставке;
                     * - deliveryTag;
                     * - свойства сообщения.
                     */


                    /*
                     * Получаем deliveryTag.
                     *
                     * deliveryTag — номер доставки этого сообщения
                     * внутри текущего Channel.
                     *
                     * Потом через этот номер мы скажем RabbitMQ:
                     *
                     * ACK:
                     * "сообщение успешно обработано"
                     *
                     * или NACK:
                     * "обработать не удалось".
                     */
                    long deliveryTag =
                            delivery
                                    .getEnvelope()
                                    .getDeliveryTag();


                    /*
                     * ВОТ ЗДЕСЬ МЫ ПОЛУЧАЕМ
                     * САМО ТЕЛО СООБЩЕНИЯ ИЗ RABBITMQ.
                     *
                     * getBody() возвращает byte[].
                     */
                    byte[] messageBytes =
                            delivery.getBody();


                    /*
                     * Наш publisher отправлял String → byte[].
                     *
                     * Теперь consumer делает обратное:
                     *
                     * byte[] → String.
                     */
                    String payload =
                            new String(
                                    messageBytes,
                                    StandardCharsets.UTF_8
                            );


                    /*
                     * Здесь payload уже содержит наши метрики.
                     *
                     * Например:
                     *
                     * network_rx_bytes{agent="agent-1",interface="eth0"} ...
                     * network_tx_bytes{agent="agent-1",interface="eth0"} ...
                     */


                    try {

                        /*
                         * ВОТ ЗДЕСЬ МЫ ОТПРАВЛЯЕМ
                         * ПОЛУЧЕННОЕ ИЗ RABBITMQ СООБЩЕНИЕ
                         * В VICTORIAMETRICS.
                         */
                        victoriaClient.send(
                                payload
                        );


                        /*
                         * Если предыдущая строка завершилась успешно,
                         * значит VictoriaMetrics приняла метрику.
                         *
                         * Теперь отправляем ACK в RabbitMQ.
                         *
                         * ACK означает:
                         *
                         * "Я успешно обработал это сообщение.
                         *  Можешь удалить его из очереди."
                         */
                        channel.basicAck(

                                /*
                                 * Какое именно сообщение подтверждаем.
                                 */
                                deliveryTag,

                                /*
                                 * multiple = false
                                 *
                                 * Подтверждаем только это сообщение,
                                 * а не несколько сразу.
                                 */
                                false
                        );


                        System.out.println(
                                "Message stored in VictoriaMetrics"
                        );


                    } catch (Exception e) {

                        /*
                         * Мы попали сюда, если:
                         *
                         * - VictoriaMetrics упала;
                         * - сеть недоступна;
                         * - HTTP request завершился ошибкой;
                         * - произошла другая ошибка обработки.
                         */

                        System.err.println(
                                "VictoriaMetrics error: "
                                        + e.getMessage()
                        );


                        /*
                         * Делаем небольшую паузу,
                         * чтобы при падении VictoriaMetrics
                         * consumer не начал очень быстро
                         * получать → ошибаться → возвращать
                         * одно и то же сообщение.
                         */
                        try {

                            Thread.sleep(2000);

                        } catch (InterruptedException interrupted) {

                            Thread.currentThread()
                                    .interrupt();
                        }


                        /*
                         * NACK означает:
                         *
                         * "Я получил сообщение,
                         *  но обработать его не смог."
                         */
                        channel.basicNack(

                                /*
                                 * Какое сообщение.
                                 */
                                deliveryTag,

                                /*
                                 * multiple = false
                                 *
                                 * Работаем только с этим сообщением.
                                 */
                                false,

                                /*
                                 * requeue = true
                                 *
                                 * ВАЖНО.
                                 *
                                 * Вернуть сообщение обратно в очередь.
                                 *
                                 * То есть:
                                 *
                                 * RabbitMQ
                                 *     ↓
                                 * Consumer
                                 *     ↓
                                 * VictoriaMetrics ❌
                                 *
                                 * сообщение возвращается:
                                 *
                                 * Consumer
                                 *     ↓
                                 * RabbitMQ
                                 */
                                true
                        );
                    }
                };


        /*
         * ВОТ ЭТА СТРОКА ПОДПИСЫВАЕТ CONSUMER НА ОЧЕРЕДЬ.
         *
         * До этой строки мы только:
         *
         * - подключились;
         * - создали Channel;
         * - описали callback.
         *
         * Именно basicConsume() говорит RabbitMQ:
         *
         * "Начинай отправлять мне сообщения
         *  из очереди network.metrics."
         */
        channel.basicConsume(

                /*
                 * Какую очередь слушать.
                 */
                queueName,

                /*
                 * autoAck = false
                 *
                 * RabbitMQ НЕ будет автоматически
                 * удалять сообщение после доставки.
                 *
                 * Мы сами должны вызвать:
                 *
                 * basicAck()
                 *
                 * или:
                 *
                 * basicNack()
                 */
                false,

                /*
                 * Что делать,
                 * когда пришло новое сообщение.
                 */
                deliverCallback,

                /*
                 * Callback на случай отмены consumer.
                 *
                 * Пока нам здесь никакая логика не нужна.
                 */
                consumerTag -> {
                }
        );


        /*
         * Добавляем обработчик остановки JVM.
         *
         * Например, если Docker остановит контейнер.
         *
         * Тогда аккуратно закроем:
         *
         * Channel
         * Connection
         */
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(() -> {

                            try {

                                channel.close();

                            } catch (Exception ignored) {
                            }

                            try {

                                connection.close();

                            } catch (Exception ignored) {
                            }
                        })
                );


        /*
         * После basicConsume() обработка сообщений
         * происходит асинхронно.
         *
         * Если сейчас main() завершится,
         * приложение остановится.
         *
         * Поэтому мы создаём CountDownLatch
         * и ждём бесконечно.
         *
         * Таким образом контейнер продолжает работать
         * и слушать RabbitMQ.
         */
        new CountDownLatch(1).await();
    }


    /*
     * Вспомогательный метод:
     *
     * пытаемся получить environment variable.
     *
     * Если её нет — используем defaultValue.
     */
    private static String getEnv(
            String name,
            String defaultValue
    ) {

        return System.getenv()
                .getOrDefault(
                        name,
                        defaultValue
                );
    }
}