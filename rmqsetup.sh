#!/usr/bin/env bash

# add a user and permissions
rabbitmqctl add_user testuser testpassword
rabbitmqctl set_user_tags testuser administrator
rabbitmqctl set_permissions -p / testuser ".*" ".*" ".*"

# make virtualhost
rabbitmqctl add_vhost Some_Virtual_Host
rabbitmqctl set_permissions -p Some_Virtual_Host testuser ".*" ".*" ".*"

# declare exchange
# NOTE: even if rabbitmqadmin is installed the plugin has to be enabled `rabbitmq-plugins enable rabbitmq_management`
rabbitmqadmin declare exchange --vhost=Some_Virtual_Host name=some_exchange type=direct
rabbitmqadmin declare queue --vhost=Some_Virtual_Host name=some_outgoing_queue durable=false
rabbitmqadmin --vhost="Some_Virtual_Host" declare binding source="some_exchange" destination_type="queue" destination="some_outgoing_queue" routing_key="some_routing_key"

rabbitmqadmin publish --vhost=Some_Virtual_Host exchange=some_exchange routing_key="some_routing_key" payload="Hello from rabbitmqadmin"
rabbitmqadmin publish --vhost=Some_Virtual_Host exchange=some_exchange routing_key="some_routing_key" payload="Message 2"
rabbitmqadmin publish --vhost=Some_Virtual_Host exchange=some_exchange routing_key="some_routing_key" payload="Message 3"
rabbitmqadmin publish --vhost=Some_Virtual_Host exchange=some_exchange routing_key="some_routing_key" payload="Message 4"
rabbitmqadmin publish --vhost=Some_Virtual_Host exchange=some_exchange routing_key="some_routing_key" payload="Message 5"
