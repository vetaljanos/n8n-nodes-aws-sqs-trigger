import {
	IDataObject,
	ILoadOptionsFunctions,
	INodeExecutionData,
	INodePropertyOptions,
	INodeType,
	INodeTypeDescription,
	ITriggerFunctions,
	ITriggerResponse,
	NodeApiError,
	NodeConnectionTypes,
	NodeOperationError
} from 'n8n-workflow';

import {
	awsApiRequestSOAP,
} from '../GenericFunctions';

// noinspection JSUnusedGlobalSymbols
export class AwsSqsTrigger implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'AWS SQS Trigger',
		name: 'awsSqsTrigger',
		icon: 'file:sqs.svg',
		group: ['trigger'],
		version: 1,
		subtitle: `={{$parameter["queue"]}}`,
		description: 'Consume queue messages from AWS SQS',
		defaults: {
			name: 'AWS SQS Trigger',
		},
		inputs: [],
		outputs: [NodeConnectionTypes.Main],
		credentials: [
			{
				name: 'aws',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Queue Name or ID',
				name: 'queue',
				type: 'options',
				typeOptions: {
					loadOptionsMethod: 'getQueues',
				},
				options: [],
				default: '',
				required: true,
				description:
					'Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>',
			},
			{
				displayName: 'Interval',
				name: 'interval',
				type: 'number',
				typeOptions: {
					minValue: 1,
				},
				default: 1,
				description: 'Interval value which the queue will be checked for new messages',
			},
			{
				displayName: 'Unit',
				name: 'unit',
				type: 'options',
				options: [
					{
						name: 'Seconds',
						value: 'seconds',
					},
					{
						name: 'Minutes',
						value: 'minutes',
					},
					{
						name: 'Hours',
						value: 'hours',
					},
				],
				default: 'seconds',
				description: 'Unit of the interval value',
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				placeholder: 'Add Option',
				default: {},
				options: [
					{
						displayName: 'Delete Messages',
						name: 'deleteMessages',
						type: 'boolean',
						default: true,
						description: 'Whether to delete messages after receiving them',
					},
					{
						displayName: 'Visibility Timeout',
						name: 'visibilityTimeout',
						type: 'number',
						default: 30,
						description:
							'The duration (in seconds) that the received messages are hidden from subsequent retrieve requests after being retrieved by a receive message request. Value must be in range 0-43200 (12 hours).',
					},
					{
						displayName: 'Max Number Of Messages',
						name: 'maxNumberOfMessages',
						type: 'number',
						default: 1,
						description:
							'Maximum number of messages to return. SQS never returns more messages than this value but might return fewer. Should be in range 1-10',
					},
					{
						displayName: 'Wait Time Seconds',
						name: 'waitTimeSeconds',
						type: 'number',
						default: 0,
						description:
							'Enable long-polling with a non-zero number of seconds. Maximum 20 seconds.',
					},
				],
			},
		],
	};

	methods = {
		loadOptions: {
			// Get all the available queues to display them to user so that it can be selected easily
			async getQueues(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
				const params = ['Version=2012-11-05', `Action=ListQueues`];

				let data;
				try {
					// loads first 1000 queues from SQS
					data = await awsApiRequestSOAP.call(this, 'sqs', 'GET', `?${params.join('&')}`);
				} catch (error) {
					throw new NodeApiError(this.getNode(), error);
				}

				let queues = data.ListQueuesResponse.ListQueuesResult.QueueUrl;
				if (!queues) {
					return [];
				}

				if (!Array.isArray(queues)) {
					// If user has only a single queue no array get returned so we make
					// one manually to be able to process everything identically
					queues = [queues];
				}

				return queues.map((queueUrl: string) => {
					const urlParts = queueUrl.split('/');
					const name = urlParts[urlParts.length - 1];

					return {
						name,
						value: queueUrl,
					};
				});
			},
		},
	};

	async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
		const queueUrl = this.getNodeParameter('queue') as string;
		const queuePath = new URL(queueUrl).pathname;

		const interval = this.getNodeParameter('interval') as number;
		const unit = this.getNodeParameter('unit') as string;

		const options = this.getNodeParameter('options', {}) as IDataObject;

		const receiveMessageParams = [
			'Version=2012-11-05',
			`Action=ReceiveMessage`,
			'MessageAttributeName=All',
			'AttributeName=All',
		];

		if (options.visibilityTimeout) {
			const visibilityTimeout = options.visibilityTimeout;

			if (typeof visibilityTimeout === 'number') {
				if (visibilityTimeout < 0 || visibilityTimeout > 43200) {
					throw new NodeOperationError(
						this.getNode(),
						'Visibility Timeout must be between 0 and 43200 (12 hours).',
					);
				}

				receiveMessageParams.push(`VisibilityTimeout=${visibilityTimeout}`);
			}

		}

		if (options.maxNumberOfMessages) {
			const maxNumberOfMessages = options.maxNumberOfMessages;

			if (typeof maxNumberOfMessages === 'number') {
				if (maxNumberOfMessages < 1 || maxNumberOfMessages > 10) {
					throw new NodeOperationError(
						this.getNode(),
						'Max Number Of Messages must be between 1 and 10.',
					);
				}

				receiveMessageParams.push(`MaxNumberOfMessages=${maxNumberOfMessages}`);
			}

		}

		if (options.waitTimeSeconds) {
			if (
				typeof options.waitTimeSeconds === 'number' &&
				(options.waitTimeSeconds < 0 || options.waitTimeSeconds > 20)
			) {
				throw new NodeOperationError(this.getNode(), 'Wait Time Seconds must be between 0 and 20.');
			}
			receiveMessageParams.push(`WaitTimeSeconds=${options.waitTimeSeconds}`);
		}

		if (interval <= 0) {
			throw new NodeOperationError(
				this.getNode(),
				'The interval has to be set to at least 1 or higher!',
			);
		}

		let intervalValue = interval;
		if (unit === 'minutes') {
			intervalValue *= 60;
		}
		if (unit === 'hours') {
			intervalValue *= 60 * 60;
		}

		intervalValue = Math.max(intervalValue, Number(options.waitTimeSeconds || 0));

		const executeTrigger = async () => {
			try {
				const responseData = await awsApiRequestSOAP.call(
					this,
					'sqs',
					'GET',
					`${queuePath}?${receiveMessageParams.join('&')}`,
				);
				const receiveMessageResult = responseData?.ReceiveMessageResponse?.ReceiveMessageResult;
				const messages = receiveMessageResult?.Message;
				if (!messages) return;

				const messagesArray = Array.isArray(messages) ? messages : [messages];

				const returnMessages: INodeExecutionData[] = messagesArray.map((json: {}) => ({ json }));

				if (options.deleteMessages) {
					const deleteMessagesParams = ['Version=2012-11-05'];

					if (messagesArray.length > 1) {
						deleteMessagesParams.push(`Action=DeleteMessageBatch`);

						for (let i = 0; i < messagesArray.length; i++) {
							const deleteMessageBatchRequestId = i + 1;
							const deleteMessageBatchRequestEntry = `DeleteMessageBatchRequestEntry.${deleteMessageBatchRequestId}`;

							deleteMessagesParams.push(
								`${deleteMessageBatchRequestEntry}.Id=msg${deleteMessageBatchRequestId}`,
								`${deleteMessageBatchRequestEntry}.ReceiptHandle=${encodeURIComponent(
									messagesArray[i].ReceiptHandle,
								)}`,
							);
						}
					} else {
						deleteMessagesParams.push(
							`Action=DeleteMessage`,
							`ReceiptHandle=${encodeURIComponent(messagesArray[0].ReceiptHandle)}`,
						);
					}

					await awsApiRequestSOAP.call(
						this,
						'sqs',
						'GET',
						`${queuePath}?${deleteMessagesParams.join('&')}`,
					);
				}

				this.emit([returnMessages]);

			} catch (error) {
				this.logger.error(`[SQS] receive/delete failed: ${error?.message || error}`);
			}
		};

		intervalValue *= 1000;

		// Reference: https://nodejs.org/api/timers.html#timers_setinterval_callback_delay_args
		if (intervalValue > 2147483647) {
			throw new NodeApiError(this.getNode(), { message: 'The interval value is too large.' });
		}

		let running = true;
		let intervalObj = setTimeout(run, 0);
		let inFlight = false;
		async function run() {
			if (inFlight) return rearm();
			inFlight = true;
			try { await executeTrigger(); }
			finally { inFlight = false; rearm(); }
		}
		function rearm() {
			if (running) intervalObj = setTimeout(run, intervalValue);
		}

		async function closeFunction() {
			running = false;
			clearTimeout(intervalObj);
		}

		return {
			closeFunction,
		};
	}
}
