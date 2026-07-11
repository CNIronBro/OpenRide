FROM rabbitmq:3.9-management

# rabbitmq-delayed-message-exchange 社区插件
RUN apt-get update && apt-get install -y wget ca-certificates && rm -rf /var/lib/apt/lists/*

RUN wget -q -P /plugins/ \
    https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/3.9.0/rabbitmq_delayed_message_exchange-3.9.0.ez

RUN echo '[rabbitmq_management,rabbitmq_management_agent,rabbitmq_web_dispatch,rabbitmq_prometheus,rabbitmq_delayed_message_exchange].' \
    > /etc/rabbitmq/enabled_plugins