using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace TesterApp
{
    class Program
    {
        private static int subscriber1Count;
        private static int subscriber2Count;

        static void Main(string[] args)
        {
            /* MSFT support case # 117041715610459 (resolved)
           Scenario

           - 2 consumer queues (unsubscribingfromevent.subscriber1 and unsubscribingfromevent.subscriber2)
           - 1 topic (bundle-1)
           - 2 subscriptions (UnsubscribingFromEvent.Subscriber1 and UnsubscribingFromEvent.Subscriber2)
              with identical single rule (18f888e1-444f-7cb7-25c8-b738fbbd1388) with filter ([NServiceBus.EnclosedMessageTypes] LIKE 'NServiceBus.AcceptanceTests.Routing.NativePublishSubscribe.When_unsubscribing_from_event+Event%' OR [NServiceBus.EnclosedMessageTypes] LIKE '%NServiceBus.AcceptanceTests.Routing.NativePublishSubscribe.When_unsubscribing_from_event+Event%' OR [NServiceBus.EnclosedMessageTypes] LIKE '%NServiceBus.AcceptanceTests.Routing.NativePublishSubscribe.When_unsubscribing_from_event+Event' OR [NServiceBus.EnclosedMessageTypes] = 'NServiceBus.AcceptanceTests.Routing.NativePublishSubscribe.When_unsubscribing_from_event+Event')

           Code creates the subscriptions. If they exists, it tries to modify those.
           A message (#1) is sent. 
           When unsubscribingfromevent.subscriber2 gets it, it unsubscribes (removed subscriptions)
           Another message (#2) is sent.
           Only consumer unsubscribingfromevent.subscriber1 should get the last 2 messages.
            */

            MainAsync().GetAwaiter().GetResult();

            Console.WriteLine("Press enter to stop");
            Console.ReadLine();
        }

        public static async Task MainAsync()
        {
            var connectionString = Environment.GetEnvironmentVariable("AzureServiceBus.ConnectionString");
            var namespaceManager1 = NamespaceManager.CreateFromConnectionString(connectionString);
            var namespaceManager2 = NamespaceManager.CreateFromConnectionString(connectionString);
            var factory = MessagingFactory.Create(namespaceManager1.Address, namespaceManager1.Settings.TokenProvider);

            var queue1Path = "UnsubscribingFromEvent.Subscriber1";
            var queue2Path = "UnsubscribingFromEvent.Subscriber2";

            var queue1Description = CreateQueueDescription(queue1Path);
            var queue2Description = CreateQueueDescription(queue2Path);
            
            // create queues and message handlers
            var queue1Client = await CreateQueueAndQueueClient(namespaceManager1, factory, queue1Path, queue1Description);
            var queue2Client = await CreateQueueAndQueueClient(namespaceManager2, factory, queue2Path, queue2Description);
            await RegisterCallbackFor(queue1Client, queue1Path, () => Interlocked.Increment(ref subscriber1Count));
            await RegisterCallbackFor(queue2Client, queue2Path, () => Interlocked.Increment(ref subscriber2Count));

            Console.WriteLine("Queues creation requested. Message handlers registered.\n");

            // create topic
            const string topicPath = "bundle-x";
          
            // create subscriptions with rules
            const string sub1Name = "UnsubscribingFromEvent.Subscriber1";
            const string sub2Name = "UnsubscribingFromEvent.Subscriber2";
            await CreateOrUpdateSubscriptionAndRule(topicPath, sub1Name, queue1Path, namespaceManager1, connectionString);
            await CreateOrUpdateSubscriptionAndRule(topicPath, sub2Name, queue2Path, namespaceManager2, connectionString);
//~            await CreateWiretap(topicPath, namespaceManager1);

            Console.WriteLine("Topic and subscriptions creation/update requested.\n");
            Console.WriteLine("Waiting for 5 seconds prior to sending a message...\n");
            await Task.Delay(5000);

            // Step 1 - send a message to be received by both subscriptions
            var topicClient = factory.CreateTopicClient(topicPath);
            var message = new BrokeredMessage("msg #1");
            message.Properties["NServiceBus.EnclosedMessageTypes"] = "NServiceBus.AcceptanceTests.Routing.NativePublishSubscribe.When_unsubscribing_from_event+Event";
            await topicClient.SendAsync(message);
            Console.WriteLine($"msg #1 sent to the topic {topicPath}.");

            // Let subscription2 received the message first
            while (subscriber2Count < 1)
            {
                await Task.Delay(1000);
            }

            // Step 2 - remove the subscription for UnsubscribingFromEvent.Subscriber2
            await namespaceManager2.DeleteSubscriptionAsync(topicPath, sub2Name);

            Console.WriteLine($"Subscription {sub2Name} has been removed.\n");

            // Step 3 - send another message to be received by UnsubscribingFromEvent.Subscriber1 only
            message = new BrokeredMessage("msg #2");
            message.Properties["NServiceBus.EnclosedMessageTypes"] = "NServiceBus.AcceptanceTests.Routing.NativePublishSubscribe.When_unsubscribing_from_event+Event";
            await topicClient.SendAsync(message);

            Console.WriteLine($"msg #2 sent to the topic {topicPath}.\n");

            Console.WriteLine("Waiting for 5 seconds...\n");
            await Task.Delay(5000);

            if (subscriber1Count != 2 || subscriber2Count != 1)
            {
                Console.WriteLine("\nRedmond, we've got a problem...");
                Console.WriteLine($"{nameof(subscriber2Count)} was expected to receive 1 messages, but got {subscriber2Count}");
                Console.WriteLine($"{nameof(subscriber1Count)} was expected to receive 2 messages, but got {subscriber1Count}");
            }

            await queue1Client.CloseAsync();
            await queue2Client.CloseAsync();
        }

        private static async Task CreateWiretap(string topicPath, NamespaceManager namespaceManager)
        {
            var name = "wiretap";
            if (await namespaceManager.SubscriptionExistsAsync(topicPath, name))
            {
                await namespaceManager.DeleteSubscriptionAsync(topicPath, name);
            }
            await namespaceManager.CreateSubscriptionAsync(topicPath, name);
            Console.WriteLine("Created wiretap.");
        }

        private static async Task CreateOrUpdateSubscriptionAndRule(string topicPath, string subName, string queuePath, NamespaceManager namespaceManager, string connectionString)
        {
            if (!await namespaceManager.TopicExistsAsync(topicPath))
            {
                await namespaceManager.CreateTopicAsync(topicPath);
            }

            var ruleName = "18f888e1-444f-7cb7-25c8-b738fbbd1388";
            var ruleSqlFilter = new SqlFilter("[NServiceBus.EnclosedMessageTypes] LIKE 'NServiceBus.AcceptanceTests.Routing.NativePublishSubscribe.When_unsubscribing_from_event+Event%' OR [NServiceBus.EnclosedMessageTypes] LIKE '%NServiceBus.AcceptanceTests.Routing.NativePublishSubscribe.When_unsubscribing_from_event+Event%' OR [NServiceBus.EnclosedMessageTypes] LIKE '%NServiceBus.AcceptanceTests.Routing.NativePublishSubscribe.When_unsubscribing_from_event+Event' OR [NServiceBus.EnclosedMessageTypes] = 'NServiceBus.AcceptanceTests.Routing.NativePublishSubscribe.When_unsubscribing_from_event+Event'");
            var ruleDescription = new RuleDescription(ruleName, ruleSqlFilter);
            var subscriptionDescription = new SubscriptionDescription(topicPath, subName)
            {
                ForwardTo = /*namespaceManager.Address + */queuePath,
                UserMetadata = $"Events {subName} is subscribed to",
                MaxDeliveryCount = 4,
                LockDuration = TimeSpan.FromSeconds(30),
                EnableBatchedOperations = true,
                EnableDeadLetteringOnFilterEvaluationExceptions = false
            };
            // create subscription
            try
            {
                if (!await namespaceManager.SubscriptionExistsAsync(topicPath, subName))
                {
                    await namespaceManager.CreateSubscriptionAsync(subscriptionDescription, ruleDescription);
                }
                else // subscription exists
                {
                    var existingSubscriptionDescription = await namespaceManager.GetSubscriptionAsync(subscriptionDescription.TopicPath, subName).ConfigureAwait(false);
                    if (MembersAreNotEqual(existingSubscriptionDescription, subscriptionDescription))
                    {
                        Console.WriteLine($"Updating subscription '{subscriptionDescription.Name}' with new description.");
                        await namespaceManager.UpdateSubscriptionAsync(subscriptionDescription);
                    }

                    // Rules can't be queried, so try to add
                   Console.WriteLine($"Adding subscription rule '{ruleDescription.Name}' for subscription '{subscriptionDescription.Name}'.");
                    try
                    {
                        var subscriptionClient = SubscriptionClient.CreateFromConnectionString(connectionString, topicPath, subName);
                        await subscriptionClient.AddRuleAsync(ruleDescription).ConfigureAwait(false);
                    }
                    catch (MessagingEntityAlreadyExistsException exception)
                    {
                       Console.WriteLine($"Rule '{ruleDescription.Name}' already exists. Response from the server: '{exception.Message}'.");
                    }
                }
            }
            catch (MessagingEntityAlreadyExistsException)
            {
               Console.WriteLine($"Subscription '{subscriptionDescription.Name}' already exists.");
            }
            catch (MessagingException ex)
            {
                var loggedMessage = $"{(ex.IsTransient ? "Transient" : "Non transient")} {ex.GetType().Name} occurred on subscription '{subscriptionDescription.Name}' creation for topic '{subscriptionDescription.TopicPath}'.";

                if (!ex.IsTransient)
                {
                   Console.WriteLine(loggedMessage + "\n" + ex);
                    throw;
                }

               Console.WriteLine(loggedMessage + "\n" + ex);
            }
        }

        private static async Task<QueueClient> CreateQueueAndQueueClient(NamespaceManager namespaceManager, MessagingFactory factory, string queuePath, QueueDescription queueDescription)
        {
            if (!await namespaceManager.QueueExistsAsync(queuePath))
            {
                await namespaceManager.CreateQueueAsync(queueDescription);
            }

            return factory.CreateQueueClient(queuePath);
        }

        private static bool MembersAreNotEqual(SubscriptionDescription existingDescription, SubscriptionDescription newDescription) =>
            existingDescription.AutoDeleteOnIdle != newDescription.AutoDeleteOnIdle
            || existingDescription.LockDuration != newDescription.LockDuration
            || existingDescription.DefaultMessageTimeToLive != newDescription.DefaultMessageTimeToLive
            || existingDescription.EnableDeadLetteringOnMessageExpiration != newDescription.EnableDeadLetteringOnMessageExpiration
            || existingDescription.EnableDeadLetteringOnFilterEvaluationExceptions != newDescription.EnableDeadLetteringOnFilterEvaluationExceptions
            || existingDescription.MaxDeliveryCount != newDescription.MaxDeliveryCount
            || existingDescription.EnableBatchedOperations != newDescription.EnableBatchedOperations
            || existingDescription.ForwardDeadLetteredMessagesTo != newDescription.ForwardDeadLetteredMessagesTo;


        private static async Task RegisterCallbackFor(QueueClient queueClient, string queuePath, Action action)
        {
            queueClient.OnMessageAsync(async message =>
            {
                Console.WriteLine($"\nReceived message on {queuePath}. Message info: body={message.GetBody<string>()} id={message.MessageId} sequence={message.SequenceNumber}.\n");
                await message.CompleteAsync();
                action();
            }, new OnMessageOptions {AutoComplete = false, MaxConcurrentCalls = 5});
        }

        private static QueueDescription CreateQueueDescription(string queue1Path) => new QueueDescription(queue1Path)
        {
            MaxDeliveryCount = 4,
            EnableBatchedOperations = true,
            LockDuration = TimeSpan.FromSeconds(30)
        };
    }
}

