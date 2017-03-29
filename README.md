## ASB.Forwarding.Issue

### Topology setup

- Two consumers/endpoints (UnsubscribingFromEvent.Subscriber1 and UnsubscribingFromEvent.Subscriber2)
- A single topic messages sent to (bundle-x)
- Two subscriptions (one per endpoint) where each subscription has a single rule. Subscriptions forward to the appropriate queues.

### The scenario

Message #1 is published to the topic with the appropreate header and value. The message satisfies both subscriptions and each subscriptions should receive a copy of the message. Then it's forwarded to the appropriate queues.

Next, subscription UnsubscribingFromEvent.Subscriber2 is removed and the message #2 is sent to the topic again. The message also satisfies the subscription and should be forwarded in this case by the UnsubscribingFromEvent.Subscriber1 subscription to the appropriate queue.

### Expected outcome

2 messages in the UnsubscribingFromEvent.Subscriber1 queue
1 message in the UnsubscribingFromEvent.Subscriber2 queue

Which is happening when topology does not exist or randomly when it does.
Re-occurring executions with the topology in place (except subscription UnsubscribingFromEvent.Subscriber2) is often failing with unexpected result - the first message is never received by the UnsubscribingFromEvent.Subscriber2 handler. 

I'm assuming that creation of entities takes time, therefore delaying message sending rather than dispatching straight after subscriptions/rules creation. The delay is 5 seconds. Tried 10 and more. Also running w/o debugger attached. When creating rules, there's no way to update those as there's no way to query. The only option is to try to add and capture an exception. When creating an entity, it also not promised that it would be there. 

The issue is - how can I guarantee that the entity is ready to accept messages? 
