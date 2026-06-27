import {
  enqueueWebhookDelivery,
  getActiveWebhookSubscribers,
  getPendingWebhookDeliveries,
  markWebhookDeliveryFailure,
  markWebhookDeliverySuccess,
} from './database';
import { RemittanceCreatedWebhookPayload, Sep24ExpiredRefundWebhookPayload, WebhookDelivery } from './types';

const MAX_RETRIES = 5;
const RETRY_BASE_MS = parseInt(process.env.WEBHOOK_RETRY_BASE_MS || '1000', 10);
const RETRY_MAX_MS = parseInt(process.env.WEBHOOK_RETRY_MAX_MS || '300000', 10);
const RETRY_JITTER_PERCENT = parseInt(process.env.WEBHOOK_RETRY_JITTER_PERCENT || '20', 10);

export class WebhookDispatcher {
  constructor(private readonly fetchImpl: typeof fetch = fetch) {}

  async dispatchRemittanceCreated(payload: RemittanceCreatedWebhookPayload): Promise<void> {
    const subscribers = await getActiveWebhookSubscribers();
    const deliveries = await Promise.all(
      subscribers.map((subscriber) =>
        enqueueWebhookDelivery('remittance.created', payload.remittance_id, subscriber, payload, MAX_RETRIES)
      )
    );

    for (const delivery of deliveries) {
      await this.attemptDelivery(delivery);
    }
  }

  async dispatchSep24ExpiredRefund(payload: Sep24ExpiredRefundWebhookPayload): Promise<void> {
    const subscribers = await getActiveWebhookSubscribers();
    const deliveries = await Promise.all(
      subscribers.map((subscriber) =>
        enqueueWebhookDelivery('sep24.expired_refund', payload.transaction_id, subscriber, payload, MAX_RETRIES)
      )
    );

    for (const delivery of deliveries) {
      await this.attemptDelivery(delivery);
    }
  }

  async retryPendingDeliveries(limit: number = 100): Promise<void> {
    const deliveries = await getPendingWebhookDeliveries(limit);
    for (const delivery of deliveries) {
      await this.attemptDelivery(delivery);
    }
  }

  private validateUrl(url: string): void {
    if (!url.startsWith('https://')) {
      throw new Error(`Webhook delivery rejected: URL must use HTTPS (received: ${url})`);
    }
  }

  private async attemptDelivery(delivery: WebhookDelivery): Promise<void> {
    const nextAttempt = delivery.attempt_count + 1;

    try {
      this.validateUrl(delivery.target_url);

      const response = await this.fetchImpl(delivery.target_url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-event-type': delivery.event_type,
          'x-attempt': String(nextAttempt),
        },
        body: JSON.stringify(delivery.payload),
      });

      if (response.ok) {
        await markWebhookDeliverySuccess(delivery.id, response.status);
        return;
      }

      await this.scheduleFailure(
        delivery,
        nextAttempt,
        `Webhook delivery failed with status ${response.status}`,
        response.status
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown webhook delivery failure';
      await this.scheduleFailure(delivery, nextAttempt, message, null);
    }
  }

  private async scheduleFailure(
    delivery: WebhookDelivery,
    nextAttempt: number,
    message: string,
    responseStatus: number | null
  ): Promise<void> {
    const nextRetryAt = new Date(Date.now() + this.retryDelayMs(nextAttempt));
    await markWebhookDeliveryFailure(
      delivery.id,
      nextAttempt,
      delivery.max_attempts,
      nextRetryAt,
      message,
      responseStatus
    );

  }

  private retryDelayMs(attempt: number): number {
    const exponentialDelay = RETRY_BASE_MS * Math.pow(2, attempt - 1);
    const capped = Math.min(exponentialDelay, RETRY_MAX_MS);
    const jitterRange = (capped * RETRY_JITTER_PERCENT) / 100;
    const jitter = (Math.random() - 0.5) * 2 * jitterRange;
    const finalDelay = Math.max(0, capped + jitter);
    console.log(`Webhook retry attempt ${attempt}: exponential=${exponentialDelay}ms, capped=${capped}ms, jitter=${jitter.toFixed(0)}ms, final=${finalDelay.toFixed(0)}ms`);
    return finalDelay;
  }
}
