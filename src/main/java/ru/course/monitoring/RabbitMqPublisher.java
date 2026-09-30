package ru.course.monitoring;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class RabbitMqPublisher implements AutoCloseable {

    /*
     * Connection — это физическое соединение Java-приложения
     * с RabbitMQ.
     *
     * Условно:
     *
     * Agent ---> TCP connection ---> RabbitMQ:5672
     *
     * Одно Connection может содержать несколько Channel.
     */
    private final Connection connection;

    /*
     * Channel — логический канал внутри Connection.
     *
     * Именно через Channel мы:
     * - создаём/объявляем очередь;
     * - отправляем сообщения;
     * - включаем publisher confirms.
     *
     * В большинстве операций RabbitMQ используется именно Channel,
     * а не Connection напрямую.
     */
    private final Channel channel;

    /*
     * Имя очереди, куда агент будет отправлять метрики.
     *
     * В нашем случае:
     * network.metrics
     */
    private final String queueName;


    public RabbitMqPublisher(
            String host,
            int port,
            String username,
            String password,
            String queueName,
            String agentName
    ) throws IOException, TimeoutException {

        this.queueName = queueName;

        /*
         * ConnectionFactory — объект, в котором мы задаём
         * настройки подключения к RabbitMQ.
         */
        ConnectionFactory factory = new ConnectionFactory();

        /*
         * В Docker host будет:
         *
         * rabbitmq
         *
         * Потому что rabbitmq — это имя service
         * в docker-compose.yml.
         */
        factory.setHost(host);

        /*
         * 5672 — стандартный AMQP порт RabbitMQ.
         *
         * Именно по нему Java-приложения общаются с брокером.
         */
        factory.setPort(port);

        factory.setUsername(username);
        factory.setPassword(password);

        /*
         * Если сетевое соединение временно оборвётся,
         * RabbitMQ Java Client попробует его восстановить.
         */
        factory.setAutomaticRecoveryEnabled(true);

        /*
         * После восстановления Connection библиотека
         * также попробует восстановить связанные объекты,
         * например очередь и Channel.
         */
        factory.setTopologyRecoveryEnabled(true);

        /*
         * Интервал между попытками восстановления соединения.
         */
        factory.setNetworkRecoveryInterval(5000);


        /*
         * ВОТ ЗДЕСЬ устанавливается реальное соединение
         * Java Agent ---> RabbitMQ.
         *
         * Например:
         *
         * agent1 ---> rabbitmq:5672
         *
         * Строка "publisher-agent-1" — просто имя соединения.
         * Его можно будет увидеть в RabbitMQ Management UI.
         */
        connection = factory.newConnection(
                "publisher-" + agentName
        );


        /*
         * Создаём Channel внутри Connection.
         *
         * Все последующие операции RabbitMQ будем делать
         * через этот объект.
         */
        channel = connection.createChannel();


        /*
         * Объявляем очередь.
         *
         * Это можно понимать так:
         *
         * "RabbitMQ, убедись, что существует очередь
         *  с именем network.metrics".
         *
         * Если очередь уже существует с теми же параметрами,
         * ничего нового не создаётся.
         */
        channel.queueDeclare(

                queueName,

                /*
                 * durable = true
                 *
                 * Очередь переживёт перезапуск RabbitMQ.
                 *
                 * То есть RabbitMQ сохранит саму структуру очереди.
                 */
                true,

                /*
                 * exclusive = false
                 *
                 * Очередь не принадлежит только этому соединению.
                 *
                 * К ней могут подключаться и другие приложения,
                 * например наш metrics-consumer.
                 */
                false,

                /*
                 * autoDelete = false
                 *
                 * Не удалять очередь автоматически,
                 * когда отключится последний consumer.
                 */
                false,

                /*
                 * Дополнительные параметры очереди.
                 *
                 * Пока нам они не нужны.
                 */
                null
        );


        /*
         * Включаем Publisher Confirms.
         *
         * После этого RabbitMQ сможет подтверждать,
         * что опубликованное сообщение было принято брокером.
         */
        channel.confirmSelect();
    }


    public void publish(String payload)
            throws IOException, InterruptedException, TimeoutException {

        /*
         * payload — это само сообщение.
         *
         * Например:
         *
         * network_rx_bytes{agent="agent-1",interface="eth0"} 12345 ...
         * network_tx_bytes{agent="agent-1",interface="eth0"} 54321 ...
         */


        /*
         * basicPublish() — КЛЮЧЕВОЕ МЕСТО.
         *
         * ИМЕННО ЗДЕСЬ сообщение отправляется
         * из Java Agent в RabbitMQ.
         */
        channel.basicPublish(

                /*
                 * Первый аргумент — exchange.
                 *
                 * Пустая строка "" означает:
                 * использовать встроенный Default Exchange.
                 *
                 * То есть отдельный Exchange мы сейчас
                 * самостоятельно не создаём.
                 */
                "",

                /*
                 * Второй аргумент — routing key.
                 *
                 * Для Default Exchange routing key должен совпадать
                 * с именем нужной очереди.
                 *
                 * queueName = "network.metrics"
                 *
                 * Поэтому сообщение попадёт в:
                 *
                 * Queue: network.metrics
                 */
                queueName,

                /*
                 * Свойства сообщения.
                 *
                 * PERSISTENT_TEXT_PLAIN означает:
                 *
                 * - сообщение является text/plain;
                 * - сообщение помечается persistent.
                 *
                 * Persistent сообщение вместе с durable queue
                 * позволяет RabbitMQ сохранять данные
                 * при перезапуске брокера.
                 */
                MessageProperties.PERSISTENT_TEXT_PLAIN,

                /*
                 * RabbitMQ передаёт тело сообщения как byte[].
                 *
                 * Поэтому наш String превращаем в массив байтов.
                 */
                payload.getBytes(StandardCharsets.UTF_8)
        );


        /*
         * После отправки ждём подтверждение RabbitMQ.
         *
         * Логика:
         *
         * Agent:
         * "RabbitMQ, ты получил сообщение?"
         *
         * RabbitMQ:
         * "Да."
         *
         * Если подтверждение не придёт за 5 секунд,
         * будет ошибка.
         *
         * Благодаря этому агент знает,
         * действительно ли брокер принял сообщение.
         */
        channel.waitForConfirmsOrDie(5000);
    }


    /*
     * AutoCloseable позволяет использовать класс так:
     *
     * try (RabbitMqPublisher publisher = ...) {
     *     ...
     * }
     *
     * Когда блок try завершится,
     * автоматически вызовется close().
     */
    @Override
    public void close() {

        /*
         * Сначала закрываем Channel.
         */
        try {

            if (channel != null && channel.isOpen()) {
                channel.close();
            }

        } catch (Exception ignored) {
        }


        /*
         * Затем закрываем физическое Connection.
         */
        try {

            if (connection != null && connection.isOpen()) {
                connection.close();
            }

        } catch (Exception ignored) {
        }
    }
}