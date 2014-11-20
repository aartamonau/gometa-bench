#/bin/sh

num_nodes=50

device=lo
rate=1000Mbit
delay=500us

on() {
    off

    tc qdisc add dev $device handle 1: root htb

    for i in $(seq $num_nodes); do
        dport=$(( 13002 + 10 * (i - 1) ))
        sport=$(( dport + 30000 ))

        out=1:1$i
        in=1:2$i

        tc class add dev $device parent 1: classid $in htb rate $rate

        tc class add dev $device parent 1: classid $out htb rate $rate
        tc qdisc add dev $device parent $out handle 1$i: netem delay $delay

        tc filter add dev $device \
           protocol ip prio 1 u32 match ip dport $dport 0xffff flowid $in
        tc filter add dev $device \
           protocol ip prio 1 u32 match ip sport $sport 0xffff flowid $out
    done
}

off() {
    tc qdisc del dev $device root 2>/dev/null
}

case $1 in
    on) on;;
    off) off;;
    *) on;;
esac
