package main

import (
	"bufio"
	"context"
	"flag"
	"os"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

var namesrv = flag.String("n", "127.0.0.1:9876", "rocketmq namesrv for example 127.0.0.1:9876")
var topic = flag.String("t", "test-topic", "rocketmq topic")
var filePath = flag.String("f", "", "rocketmq message.txt for example /home/work/message.txt")

// Package main implements a simple producer to send message.txt.
func main() {
	flag.Parse()
	rlog.Info("read command content:", map[string]interface{}{"namesrv": *namesrv, "topic": *topic, "messageFile": *filePath})

	if "" == *filePath {
		rlog.Error("message file of path is empty!!!", nil)
		os.Exit(1)
	}
	msgFile, e := os.OpenFile(*filePath, os.O_RDONLY, 0644)
	if e != nil {
		rlog.Error("open file error", map[string]interface{}{"": e.Error()})
		os.Exit(1)
	}
	defer msgFile.Close()

	p, _ := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{*namesrv})),
		producer.WithRetry(2),
	)
	err := p.Start()
	if err != nil {
		rlog.Error("start producer error", map[string]interface{}{"": err.Error()})
		os.Exit(1)
	}

	reader := bufio.NewScanner(msgFile)
	for reader.Scan() {
		line := reader.Text()
		sendMessage(p, *topic, line)
	}

	err = msgFile.Close()
	if err != nil {
		rlog.Error("close file failed", map[string]interface{}{"error": err.Error()})
	}

	err = p.Shutdown()
	if err != nil {
		rlog.Error("shutdown producer", map[string]interface{}{"error": err.Error()})
	}

}
func sendMessage(p rocketmq.Producer, topic string, body string) {
	msg := &primitive.Message{
		Topic: topic,
		Body:  []byte(body),
	}
	res, err := p.SendSync(context.Background(), msg)

	if err != nil {
		rlog.Error("send message failed", map[string]interface{}{"error": err, "message": body})
	} else {
		rlog.Info("send message success", map[string]interface{}{"result": res.String(), "message": body})
	}
}
