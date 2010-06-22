#!/bin/sh

RMQCTL=rabbitmqctl
$RMQCTL delete_vhost /hive
$RMQCTL delete_user hive
$RMQCTL add_user hive resistanceisfutile
$RMQCTL add_vhost /hive
$RMQCTL set_permissions -p /hive hive '.*' '.*' '.*'



