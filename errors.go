package workqueue

import "errors"

var (
	QueueIsStarted  = errors.New("Queue is already started")
	QueueNotStarted = errors.New("Queue is not started yet")
	QueueIsStopping = errors.New("Queue is in the process of stopping")
)
