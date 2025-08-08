import { ActionFunctionArgs } from '@remix-run/node';
import { authenticate } from '../shopify.server';
import prisma from '../db.server';
import { Prisma } from '@prisma/client'; // Removed PrismaClient unless needed elsewhere
// Import shared payload types
import type { PayloadOrder } from '../types/shopify.payload.types.js';
// Import simplified job data type
import {
  orderProcessingQueue,
  type OrderJobData,
} from '../queues/orderProcessingQueue.server.js';
import { log } from '../utils/logger.server';

// --- Removed Local Payload Type Definitions ---

// Define a type for the expected structure returned by authenticate.webhook
// This might need adjustment based on the actual library implementation
interface AuthenticatedWebhookContext {
  shop: string;
  topic: string;
  payload: Record<string, unknown>; // Use a general object type initially
}

/**
 * Webhook handler for the 'orders/create' topic.
 * Authenticates the request, stores the raw event, validates essential data,
 * and adds a job to the BullMQ queue for background processing.
 */
export const action = async ({
  request,
}: ActionFunctionArgs): Promise<Response> => {
  const webhookReceivedTime = Date.now();
  let shop: string | undefined;
  let topic: string | undefined;
  let eventIdHeader: string | null = request.headers.get(
    'X-Shopify-Webhook-Id'
  );
  let shopifyOrderIdNum: number | undefined;

  try {
    log.info(undefined, '[Webhook Handler] 1. Received orders/create webhook');
    console.time('[Webhook Handler] Authentication');
    const webhookContext = (await authenticate.webhook(
      request
    )) as unknown as AuthenticatedWebhookContext;
    console.timeEnd('[Webhook Handler] Authentication');
    const {
      shop: authenticatedShop,
      topic: authenticatedTopic,
      payload,
    } = webhookContext;

    shop = authenticatedShop;
    topic = authenticatedTopic;

    if (!shop || !topic || !payload || typeof payload !== 'object') {
      log.error(
        { shop, topic },
        '[Webhook Handler] Invalid webhook context or missing payload.'
      );
      return new Response('Invalid payload', { status: 200 });
    }
    if (typeof payload.id !== 'number') {
      log.error(
        { shop, topic },
        '[Webhook Handler] Missing Shopify Order ID in payload.'
      );
      return new Response('Invalid payload - Missing Order ID', {
        status: 200,
      });
    }
    shopifyOrderIdNum = payload.id as number;

    log.info(
      { shop, topic, shopifyOrderIdNum },
      '[Webhook Handler] 2. Webhook authenticated'
    );

    const normalizedTopic = topic.toLowerCase().replace(/_/g, '/');
    if (normalizedTopic !== 'orders/create') {
      log.warn(
        { shop, topic, shopifyOrderIdNum },
        '[Webhook Handler] Unexpected topic, skipping.'
      );
      return new Response(null, { status: 200 });
    }

    const eventId =
      eventIdHeader ?? `generated_${Date.now()}_${shopifyOrderIdNum}`;
    log.info(
      { shop, topic, eventId },
      '[Webhook Handler] 3. Storing webhook event'
    );

    const webhookEvent = await prisma.webhookEvent.create({
      data: {
        shopId: shop,
        topic: normalizedTopic,
        payload: payload as Prisma.InputJsonValue,
        processed: false,
        eventId: eventId,
      },
    });
    log.info(
      { shop, topic, eventId, webhookEventId: webhookEvent.id },
      '[Webhook Handler] 4. Webhook event stored in DB'
    );

    // Cast payload to specific type *after* saving raw event
    const orderPayload = payload as unknown as PayloadOrder;

    // Validate Phone Number before queuing
    const customerPhone =
      orderPayload.phone ??
      orderPayload.shipping_address?.phone ??
      orderPayload.billing_address?.phone ??
      orderPayload.customer?.phone;

    if (!customerPhone) {
      log.warn(
        { shop, shopifyOrderIdNum },
        '[Webhook Handler] 5a. No phone number, skipping queue add'
      );
      await prisma.webhookEvent.update({
        where: { id: webhookEvent.id },
        data: {
          processed: true, // Mark as processed (skipped)
          processedAt: new Date(),
          lastError: 'Skipped: No customer phone number found in payload.',
        },
      });
      return new Response('Acknowledged, no phone number.', { status: 200 });
    }

    log.info(
      { shop, shopifyOrderIdNum, customerPhone },
      '[Webhook Handler] 5. Phone number found'
    );

    // --- Use Simplified Job Data ---
    const jobData: OrderJobData = {
      webhookEventId: webhookEvent.id,
    };

    const jobId = `order-${shop}-${shopifyOrderIdNum}`;
    const job = await orderProcessingQueue.add(
      `order-create-${shopifyOrderIdNum}`,
      jobData,
      {
        jobId: jobId,
        // Add timestamp to track queue delay
        timestamp: webhookReceivedTime,
      }
    );
    const queueAddTime = Date.now() - webhookReceivedTime;
    log.info(
      { shop, shopifyOrderIdNum, jobId: job.id, queueAddMs: queueAddTime },
      '[Webhook Handler] 6. Job added to queue'
    );

    // Optional: Update WebhookEvent status to QUEUED (removed linking job.id)
    await prisma.webhookEvent.update({
      where: { id: webhookEvent.id },
      data: {
        // Consider status: 'QUEUED'
      },
    });

    const totalWebhookTime = Date.now() - webhookReceivedTime;
    log.info(
      { shop, totalWebhookMs: totalWebhookTime },
      '[Webhook Handler] 7. Responding 200 OK to Shopify'
    );
    return new Response(null, { status: 200 });
  } catch (error: unknown) {
    log.error(
      { shop, topic, shopifyOrderIdNum },
      '[Webhook Handler] CRITICAL ERROR',
      error
    );
    let errorMessage = 'Unknown error occurred';
    if (error instanceof Error) {
      errorMessage = error.message;
      log.error(
        { shop, topic, shopifyOrderIdNum },
        '[Webhook Handler] Error processing webhook',
        errorMessage
      );
    }
    return new Response('Error processing webhook internally, acknowledged.', {
      status: 200,
    });
  }
};

// --- Removed processWebhookQueue function ---
// The processing logic is now handled by the BullMQ worker
// async function processWebhookQueue(): Promise<void> { ... }
