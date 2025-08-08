import { Worker, type Job } from 'bullmq';
import prisma from '../db.server.js';
import {
  ORDER_PROCESSING_QUEUE_NAME,
  type OrderJobData,
} from '../queues/orderProcessingQueue.server.js';
import redisConfig from '../config/redis.server.js';
import pkg from 'twilio';
import { Prisma } from '@prisma/client'; // Import Prisma types
import type { PayloadOrder } from '../types/shopify.payload.types.js';
// Import the User3fa and DigitalID services
import { User3faService } from '../services/user/index.js';
import { DigitalIdService } from '../services/digital-id/index.server.js';
import { log } from '../utils/logger.server.js';

const { Twilio } = pkg;

// Initialize services
const user3faService = new User3faService();
const digitalIdService = new DigitalIdService();

/**
 * @file orderProcessor.worker.ts
 * @description BullMQ Worker for processing Shopify order creation webhooks.
 *
 * This worker listens to the 'order-processing' queue, which receives jobs
 * when a new order is created in a Shopify store. Each job contains a
 * `webhookEventId` that references a `WebhookEvent` record in the database,
 * which in turn stores the raw payload of the Shopify 'orders/create' webhook.
 *
 * Main Responsibilities:
 * 1.  **Fetch Webhook Data**: Retrieves the full webhook payload from the
 *     `WebhookEvent` table using the `webhookEventId` from the job data.
 * 2.  **Find or Create User3fa**: Identify or create a User3fa record.
 * 3.  **Create Orders3fa and Send SMS Early**: Create a 3FA-specific order record
 *     and send the SMS with a link to it early in the process.
 * 4.  **Provision Privy Wallet**: Ensure a wallet and DID are created.
 * 5.  **Create UniqueDigitalIds**: Generate digital IDs for purchased items.
 * 6.  **Save Data to Database**: Mirror the Shopify data in our database.
 * 7.  **Update WebhookEvent Status**: Mark the event as processed upon completion.
 */

// --- Initialize Twilio Client ---
const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken = process.env.TWILIO_AUTH_TOKEN;
const twilioPhoneNumber = process.env.TWILIO_PHONE_NUMBER;

let twilioClient: any | null = null;
if (accountSid && authToken && twilioPhoneNumber) {
  try {
    twilioClient = new Twilio(accountSid, authToken);
    console.log('✅ Twilio Client Initialized.');
  } catch (error) {
    console.error('🔴 Failed to initialize Twilio Client:', error);
    console.warn(
      '⚠️ Proceeding with Twilio SMS simulation due to initialization error.'
    );
  }
} else {
  console.warn('⚠️ Twilio credentials not set. SMS sending will be simulated.');
}
// --- End Twilio Client Initialization ---

// Validate Redis configuration is loaded for the worker
if (!redisConfig.connection) {
  console.error(
    '🔴 Worker: Redis connection details are not configured. Worker cannot start.'
  );
  process.exit(1); // Exit if Redis isn't configured
}

/**
 * Safely convert a price string for Decimal type compatibility.
 * Handles null/undefined/empty strings, returning "0" as default.
 * Assumes NULL bytes are already removed by an earlier process.
 */
function sanitizePriceValue(priceStr: string | null | undefined): string {
  if (priceStr === null || priceStr === undefined) {
    return '0';
  }

  // Convert to string and trim whitespace
  const stringPrice = String(priceStr).trim();

  // Return "0" for empty string, otherwise return the trimmed string
  return stringPrice === '' ? '0' : stringPrice;
}

/**
 * The main processing function for each job in the queue.
 * Fetches the webhook payload, sends the SMS, saves order data, and updates the WebhookEvent status.
 */
const processor = async (job: Job<OrderJobData>) => {
  const jobStartTime = Date.now();
  const { webhookEventId } = job.data;
  const ctx = { jobId: job.id, webhookEventId };
  let webhookEvent: Awaited<
    ReturnType<typeof prisma.webhookEvent.findUnique>
  > | null = null;
  let orders3fa: any = null; // Define orders3fa here to be accessible in catch block

  // Add timestamp to all logs
  const timestamp = new Date().toISOString();
  log.info(ctx, `${timestamp} ⏰ Job picked up from queue`);

  // Calculate queue delay if timestamp is available
  const jobTimestamp = (job.opts as any)?.timestamp;
  if (jobTimestamp) {
    const queueDelay = jobStartTime - jobTimestamp;
    log.info(ctx, `📊 Queue waiting time: ${queueDelay}ms`);
  }

  // Also check job creation time
  const jobCreatedAt = new Date(job.timestamp).getTime();
  const totalDelay = jobStartTime - jobCreatedAt;
  log.info(ctx, `📊 Total delay from job creation: ${totalDelay}ms`);

  log.info(ctx, `Processing started - Attempt #${job.attemptsMade + 1}`);

  try {
    // 1. Fetch WebhookEvent & Order Payload
    webhookEvent = await prisma.webhookEvent.findUnique({
      where: { id: webhookEventId },
    });
    if (!webhookEvent)
      throw new Error(`WebhookEvent ${webhookEventId} not found.`);
    if (!webhookEvent.payload || typeof webhookEvent.payload !== 'object') {
      throw new Error(
        `WebhookEvent ${webhookEventId} has invalid or missing payload.`
      );
    }
    const orderPayload = webhookEvent.payload as unknown as PayloadOrder;
    log.info(ctx, 'WebhookEvent payload loaded.');

    const shopDomain = webhookEvent.shopId;
    const shopifyOrderId = String(orderPayload.id);
    const customerPhone =
      orderPayload.phone ??
      orderPayload.shipping_address?.phone ??
      orderPayload.billing_address?.phone ??
      orderPayload.customer?.phone;
    const detailedCtx = { ...ctx, shopifyOrderId, shop: shopDomain };
    log.info(detailedCtx, 'Initializing processing...');

    if (!customerPhone) {
      log.warn(detailedCtx, 'WARNING: No customer phone found in payload.');
    }

    // --- PRE-PROCESSING: Upsert ShopifyCustomer IF customer data exists in payload ---
    let customerShopifyIdAsBigInt: bigint | null = null;
    let createdOrUpdatedShopifyCustomerId: string | undefined = undefined;
    let shopifyCustomerGidForUser3fa: string | undefined = undefined;

    if (orderPayload.customer && typeof orderPayload.customer.id === 'number') {
      customerShopifyIdAsBigInt = BigInt(orderPayload.customer.id);
      const customerData = orderPayload.customer!;
      shopifyCustomerGidForUser3fa = `gid://shopify/Customer/${customerShopifyIdAsBigInt}`;
      log.info(
        detailedCtx,
        `Upserting ShopifyCustomer ${shopifyCustomerGidForUser3fa}...`
      );
      const upsertedShopifyCustomer = await prisma.shopifyCustomer.upsert({
        where: {
          shopId_shopifyId: {
            shopId: shopDomain,
            shopifyId: customerShopifyIdAsBigInt,
          },
        },
        update: {
          email: customerData.email,
          firstName: customerData.first_name,
          lastName: customerData.last_name,
          phone: customerData.phone,
          shopifyCustomerGid: shopifyCustomerGidForUser3fa,
        },
        create: {
          shopId: shopDomain,
          shopifyId: customerShopifyIdAsBigInt,
          email: customerData.email,
          firstName: customerData.first_name,
          lastName: customerData.last_name,
          phone: customerData.phone,
          shopifyCustomerGid: shopifyCustomerGidForUser3fa,
        },
        select: { id: true },
      });
      createdOrUpdatedShopifyCustomerId = upsertedShopifyCustomer.id;
      log.info(
        detailedCtx,
        `ShopifyCustomer upserted with DB ID ${createdOrUpdatedShopifyCustomerId}.`
      );
    } else {
      log.info(
        detailedCtx,
        'No Shopify customer data in payload. Skipping ShopifyCustomer upsert.'
      );
    }

    // --- 1. Find or Create User3fa ---
    console.time(`${JSON.stringify(detailedCtx)} User3fa lookup`);
    let user3fa: any = null;
    // Prepare data for User3fa service, ensuring shopifyId is correctly typed or null
    const user3faServicePayload = {
      shopifyId: customerShopifyIdAsBigInt, // This can be null
      email: orderPayload.customer?.email || orderPayload.email,
      firstName:
        orderPayload.customer?.first_name ||
        orderPayload.shipping_address?.first_name,
      lastName:
        orderPayload.customer?.last_name ||
        orderPayload.shipping_address?.last_name,
      phone: customerPhone || undefined,
      shopifyCustomerGid: shopifyCustomerGidForUser3fa,
    };

    log.info(detailedCtx, 'Finding or creating User3fa...');
    user3fa = await user3faService.findOrCreateFromShopifyCustomer(
      user3faServicePayload,
      customerPhone || undefined // Pass validated phone number for primary use
    );
    log.info({ ...detailedCtx, user3faId: user3fa?.id }, 'Using User3fa ID');
    console.timeEnd(`${JSON.stringify(detailedCtx)} User3fa lookup`);

    // --- Pre-fetch brand name to avoid delay after Orders3fa creation ---
    let brandName = 'Alliance';
    let productName = 'items';
    const rewardsPoints = 175;

    // Prepare product name early
    if (
      orderPayload.line_items &&
      orderPayload.line_items.length > 0 &&
      orderPayload.line_items[0].title
    ) {
      productName = orderPayload.line_items[0].title;
    }

    // Pre-fetch brand name
    try {
      log.info(detailedCtx, `Pre-fetching brand name for ${shopDomain}...`);
      const brand3fa = await prisma.brands3fa.findFirst({
        where: { myshopifyDomain: shopDomain },
        select: { name: true },
      });
      if (brand3fa && brand3fa.name) {
        brandName = brand3fa.name;
        log.info(detailedCtx, `Using brand name: ${brandName}`);
      }
    } catch (err) {
      log.warn(
        detailedCtx,
        `Error pre-fetching brand name, using default: ${err}`
      );
    }

    // --- 2. Create Orders3fa Early ---
    console.time(`${JSON.stringify(detailedCtx)} Orders3fa creation`);
    if (user3fa) {
      log.info(
        { ...detailedCtx, user3faId: user3fa.id },
        'Creating Orders3fa record...'
      );
      const totalItems =
        orderPayload.line_items?.reduce(
          (sum, item) => sum + item.quantity,
          0
        ) || 0;
      orders3fa = await prisma.orders3fa.create({
        data: {
          user3faId: user3fa.id,
          shopifyOrderId: shopifyOrderId, // Store Shopify Order GID
          status: 'PROCESSING',
          totalItems: totalItems,
        },
      });
      log.info(
        { ...detailedCtx, orders3faId: orders3fa.id },
        'Created Orders3fa record'
      );
      console.timeEnd(`${JSON.stringify(detailedCtx)} Orders3fa creation`);
    } else {
      log.error(
        detailedCtx,
        'CRITICAL: User3fa not found or created. Cannot proceed.'
      );
      await prisma.webhookEvent.update({
        where: { id: webhookEventId },
        data: {
          lastError: 'User3fa could not be established.',
          errorCount: { increment: 1 },
        },
      });
      throw new Error(
        'User3fa could not be established, cannot create Orders3fa.'
      );
    }

    // --- 3. Send SMS immediately after Orders3fa creation ---
    console.time(`${JSON.stringify(detailedCtx)} SMS send`);
    const timeToSMS = Date.now() - jobStartTime;
    log.info(detailedCtx, `Time from job start to SMS send: ${timeToSMS}ms`);

    if (orders3fa && customerPhone) {
      // Send SMS as soon as possible
      const passportUrl = `mvp.3fa.co/order/${orders3fa.id}`;
      const messageBody = `🛍️ Your ${brandName} Rewards (${rewardsPoints} pts). Thanks for your purchase, your ${productName} is on its way! \n\nView your product passport 🎫 here: ${passportUrl}\n\n✨ Earn for sharing\n🚚 Track shipping\n📦 Manage returns`;

      // Send SMS synchronously for simplicity and immediate delivery
      if (twilioClient && twilioPhoneNumber) {
        log.info(
          { ...detailedCtx, to: customerPhone },
          'Sending SMS via Twilio...'
        );
        try {
          const message = await twilioClient.messages.create({
            body: messageBody,
            from: twilioPhoneNumber,
            to: customerPhone,
          });
          log.info(
            { ...detailedCtx, sid: message.sid },
            '✅ Twilio SMS sent successfully'
          );
          await prisma.orders3fa.update({
            where: { id: orders3fa.id },
            data: { smsNotificationSentAt: new Date() },
          });
        } catch (twilioError: any) {
          let specificErrorMsg = twilioError.message;
          if (twilioError.code === 21211) {
            specificErrorMsg = `Invalid 'To' phone number (${customerPhone}). ${twilioError.message}`;
          }
          log.error(
            detailedCtx,
            `Twilio API error (Code: ${twilioError.code}): ${specificErrorMsg}. Proceeding.`
          );
        }
      } else {
        log.info(
          { ...detailedCtx, to: customerPhone },
          'Simulating SMS send: Twilio not configured.'
        );
        log.info(
          { ...detailedCtx, to: customerPhone },
          `<< SIMULATED SMS SUCCESS >> Body omitted`
        );
        await prisma.orders3fa.update({
          where: { id: orders3fa.id },
          data: { smsNotificationSentAt: new Date() },
        });
      }
      console.timeEnd(`${JSON.stringify(detailedCtx)} SMS send`);

      // Now continue with wallet provisioning and other operations
    } else if (!customerPhone) {
      log.info(
        detailedCtx,
        'Skipping SMS send: No customer phone number found.'
      );
    }

    // --- 4. Provision Privy Wallet (after SMS) ---
    console.time(`${JSON.stringify(detailedCtx)} Privy wallet provisioning`);
    let walletAddressForDigitalId: string | undefined | null = null;
    let privyDidForDigitalId: string | undefined | null = null;
    if (user3fa && customerPhone && orders3fa) {
      log.info(detailedCtx, `Ensuring Privy wallet for user ${user3fa.id}...`);
      await prisma.orders3fa.update({
        where: { id: orders3fa.id },
        data: { status: 'PENDING_WALLET' },
      });
      const privyResult = await user3faService.ensurePrivyWalletForUser(
        user3fa.id,
        customerPhone
      );
      if (privyResult.success) {
        walletAddressForDigitalId = privyResult.privyWalletAddress;
        privyDidForDigitalId = privyResult.privyDid;
        log.info(
          detailedCtx,
          `Privy wallet provisioned for user ${user3fa.id}. Address: ${walletAddressForDigitalId}, DID: ${privyDidForDigitalId}`
        );
        await prisma.orders3fa.update({
          where: { id: orders3fa.id },
          data: { status: 'WALLET_PROVISIONED' },
        });
      } else {
        log.warn(
          detailedCtx,
          `Privy wallet provisioning failed for user ${user3fa.id}: ${privyResult.error}`
        );
        await prisma.orders3fa.update({
          where: { id: orders3fa.id },
          data: { status: 'FAILED_WALLET_PROVISIONING' },
        });
      }
    } else if (orders3fa) {
      log.warn(
        detailedCtx,
        'Cannot ensure Privy wallet - missing user3fa or phone number'
      );
      await prisma.orders3fa.update({
        where: { id: orders3fa.id },
        data: { status: 'FAILED_NO_PHONE_FOR_WALLET' },
      });
    }
    console.timeEnd(`${JSON.stringify(detailedCtx)} Privy wallet provisioning`);

    // --- 5. Save Shopify Order and Line Items (Shopify Data Mirroring) ---
    console.time(`${JSON.stringify(detailedCtx)} Shopify data save`);
    log.info(detailedCtx, 'Saving Shopify Order and Line Items to database...');
    let orderDbId: any = null;
    let orderShopifyIdAsBigInt: bigint | null = null;
    if (typeof orderPayload.id === 'number') {
      orderShopifyIdAsBigInt = BigInt(orderPayload.id);
    }

    if (orderShopifyIdAsBigInt) {
      orderDbId = await prisma.shopifyOrder.upsert({
        where: {
          shopId_shopifyId: {
            shopId: shopDomain,
            shopifyId: orderShopifyIdAsBigInt,
          },
        },
        update: {
          email: orderPayload.email,
          phone: orderPayload.phone,
          updatedAt: orderPayload.updated_at
            ? new Date(orderPayload.updated_at)
            : new Date(),
          cancelledAt: orderPayload.cancelled_at
            ? new Date(orderPayload.cancelled_at)
            : null,
          processedAt: orderPayload.processed_at
            ? new Date(orderPayload.processed_at)
            : null,
          financialStatus: orderPayload.financial_status,
          fulfillmentStatus: orderPayload.fulfillment_status,
          totalPrice: new Prisma.Decimal(
            sanitizePriceValue(orderPayload.total_price)
          ),
          subtotalPrice: new Prisma.Decimal(
            sanitizePriceValue(orderPayload.subtotal_price)
          ),
          totalTax: new Prisma.Decimal(
            sanitizePriceValue(orderPayload.total_tax)
          ),
          total_discounts: orderPayload.total_discounts
            ? new Prisma.Decimal(
                sanitizePriceValue(orderPayload.total_discounts)
              )
            : null,
          total_line_items_price: orderPayload.total_line_items_price
            ? new Prisma.Decimal(
                sanitizePriceValue(orderPayload.total_line_items_price)
              )
            : null,
          total_outstanding: orderPayload.total_outstanding
            ? new Prisma.Decimal(
                sanitizePriceValue(orderPayload.total_outstanding)
              )
            : null,
          confirmed: orderPayload.confirmed,
          tags: orderPayload.tags,
          note: orderPayload.note,
          buyer_accepts_marketing: orderPayload.buyer_accepts_marketing,
          billing_first_name: orderPayload.billing_address?.first_name,
          billing_last_name: orderPayload.billing_address?.last_name,
          billing_address1: orderPayload.billing_address?.address1,
          billing_address2: orderPayload.billing_address?.address2,
          billing_city: orderPayload.billing_address?.city,
          billing_zip: orderPayload.billing_address?.zip,
          billing_province: orderPayload.billing_address?.province,
          billing_country: orderPayload.billing_address?.country,
          billing_phone: orderPayload.billing_address?.phone,
          billing_company: orderPayload.billing_address?.company,
          shipping_first_name: orderPayload.shipping_address?.first_name,
          shipping_last_name: orderPayload.shipping_address?.last_name,
          shipping_address1: orderPayload.shipping_address?.address1,
          shipping_address2: orderPayload.shipping_address?.address2,
          shipping_city: orderPayload.shipping_address?.city,
          shipping_zip: orderPayload.shipping_address?.zip,
          shipping_province: orderPayload.shipping_address?.province,
          shipping_country: orderPayload.shipping_address?.country,
          shipping_phone: orderPayload.shipping_address?.phone,
          shipping_company: orderPayload.shipping_address?.company,
          customerId: createdOrUpdatedShopifyCustomerId,
        },
        create: {
          shopId: shopDomain,
          shopifyId: orderShopifyIdAsBigInt,
          orderNumber: orderPayload.order_number,
          email: orderPayload.email,
          phone: orderPayload.phone,
          createdAt: new Date(orderPayload.created_at),
          cancelledAt: orderPayload.cancelled_at
            ? new Date(orderPayload.cancelled_at)
            : null,
          processedAt: orderPayload.processed_at
            ? new Date(orderPayload.processed_at)
            : null,
          currency: orderPayload.currency,
          financialStatus: orderPayload.financial_status,
          fulfillmentStatus: orderPayload.fulfillment_status,
          totalPrice: new Prisma.Decimal(
            sanitizePriceValue(orderPayload.total_price)
          ),
          subtotalPrice: new Prisma.Decimal(
            sanitizePriceValue(orderPayload.subtotal_price)
          ),
          totalTax: new Prisma.Decimal(
            sanitizePriceValue(orderPayload.total_tax)
          ),
          total_discounts: orderPayload.total_discounts
            ? new Prisma.Decimal(
                sanitizePriceValue(orderPayload.total_discounts)
              )
            : null,
          total_line_items_price: orderPayload.total_line_items_price
            ? new Prisma.Decimal(
                sanitizePriceValue(orderPayload.total_line_items_price)
              )
            : null,
          total_outstanding: orderPayload.total_outstanding
            ? new Prisma.Decimal(
                sanitizePriceValue(orderPayload.total_outstanding)
              )
            : null,
          presentment_currency: orderPayload.presentment_currency,
          payment_gateway_names: orderPayload.payment_gateway_names?.join(', '),
          confirmed: orderPayload.confirmed,
          test: orderPayload.test,
          tags: orderPayload.tags,
          note: orderPayload.note,
          source_name: orderPayload.source_name,
          customer_locale: orderPayload.customer_locale,
          buyer_accepts_marketing: orderPayload.buyer_accepts_marketing,
          billing_first_name: orderPayload.billing_address?.first_name,
          billing_last_name: orderPayload.billing_address?.last_name,
          billing_address1: orderPayload.billing_address?.address1,
          billing_address2: orderPayload.billing_address?.address2,
          billing_city: orderPayload.billing_address?.city,
          billing_zip: orderPayload.billing_address?.zip,
          billing_province: orderPayload.billing_address?.province,
          billing_country: orderPayload.billing_address?.country,
          billing_phone: orderPayload.billing_address?.phone,
          billing_company: orderPayload.billing_address?.company,
          shipping_first_name: orderPayload.shipping_address?.first_name,
          shipping_last_name: orderPayload.shipping_address?.last_name,
          shipping_address1: orderPayload.shipping_address?.address1,
          shipping_address2: orderPayload.shipping_address?.address2,
          shipping_city: orderPayload.shipping_address?.city,
          shipping_zip: orderPayload.shipping_address?.zip,
          shipping_province: orderPayload.shipping_address?.province,
          shipping_country: orderPayload.shipping_address?.country,
          shipping_phone: orderPayload.shipping_address?.phone,
          shipping_company: orderPayload.shipping_address?.company,
          customerId: createdOrUpdatedShopifyCustomerId,
        },
        select: { id: true },
      });
      log.info(
        detailedCtx,
        `ShopifyOrder ${orderShopifyIdAsBigInt} upserted with DB ID ${orderDbId.id}.`
      );

      if (orderPayload.line_items && orderPayload.line_items.length > 0) {
        log.info(
          detailedCtx,
          `Upserting ${orderPayload.line_items.length} ShopifyLineItems for order ${orderDbId.id}...`
        );
        await prisma.shopifyLineItem.deleteMany({
          where: { orderId: orderDbId.id },
        });
        const lineItemsToCreate = orderPayload.line_items.map(item => ({
          orderId: orderDbId.id,
          shopifyId: BigInt(item.id),
          title: item.title,
          price: new Prisma.Decimal(sanitizePriceValue(item.price)),
          quantity: item.quantity,
          sku: item.sku,
          productId: item.product_id ? BigInt(item.product_id) : null,
          variantId: item.variant_id ? BigInt(item.variant_id) : null,
          requiresShipping: item.requires_shipping,
          taxable: item.taxable,
        }));
        await prisma.shopifyLineItem.createMany({ data: lineItemsToCreate });
        log.info(detailedCtx, 'ShopifyLineItems created successfully.');
      } else {
        log.info(detailedCtx, 'No line items in payload.');
      }
    } else {
      throw new Error(
        `${detailedCtx.shopifyOrderId} Order payload is missing a valid numeric ID. Cannot save ShopifyOrder.`
      );
    }
    console.timeEnd(`${JSON.stringify(detailedCtx)} Shopify data save`);

    // --- 6. Create Digital IDs ---
    console.time(`${JSON.stringify(detailedCtx)} Digital ID creation`);
    if (user3fa && orders3fa && orderDbId) {
      const lineItemsForDigitalIds = await prisma.shopifyLineItem.findMany({
        where: { orderId: orderDbId.id },
      });
      log.info(
        detailedCtx,
        `Creating digital IDs for ${lineItemsForDigitalIds.length} line item records, with orders3faId: ${orders3fa.id}`
      );
      try {
        const digitalIdResults =
          await digitalIdService.createDigitalIdsForOrder({
            shopId: shopDomain,
            shopifyOrderId: shopifyOrderId,
            lineItems: lineItemsForDigitalIds,
            owner3faId: user3fa.id,
            privyWalletAddress: walletAddressForDigitalId,
            privyDid: privyDidForDigitalId,
            orders3faId: orders3fa.id,
          });
        log.info(
          detailedCtx,
          `Digital ID creation results: ${digitalIdResults.created} created, ${digitalIdResults.failed} failed`
        );
        if (digitalIdResults.failed === 0) {
          await prisma.orders3fa.update({
            where: { id: orders3fa.id },
            data: { status: 'COMPLETED' },
          });
          log.info(
            detailedCtx,
            `Orders3fa processing fully completed: ${orders3fa.id}`
          );
        } else if (digitalIdResults.created > 0) {
          await prisma.orders3fa.update({
            where: { id: orders3fa.id },
            data: { status: 'PARTIALLY_COMPLETED' },
          });
          log.info(
            detailedCtx,
            `Orders3fa processing partially completed: ${orders3fa.id}`
          );
        } else {
          await prisma.orders3fa.update({
            where: { id: orders3fa.id },
            data: { status: 'FAILED_UDI_CREATION' },
          });
          log.info(
            detailedCtx,
            `Orders3fa processing failed at UDI creation: ${orders3fa.id}`
          );
        }
      } catch (error) {
        log.error(
          detailedCtx,
          'Error creating digital IDs:',
          error instanceof Error ? error.message : 'Unknown error'
        );
        await prisma.orders3fa.update({
          where: { id: orders3fa.id },
          data: { status: 'FAILED_UDI_CREATION_ERROR' },
        });
      }
    } else {
      log.info(
        detailedCtx,
        'Skipping Digital ID creation - missing user3fa, orders3fa, or Shopify order DB ID'
      );
      if (orders3fa) {
        await prisma.orders3fa.update({
          where: { id: orders3fa.id },
          data: { status: 'FAILED_PREREQUISITES_FOR_UDI' },
        });
      }
    }
    console.timeEnd(`${JSON.stringify(detailedCtx)} Digital ID creation`);

    // --- 7. Update WebhookEvent Status on Success ---
    log.info(detailedCtx, 'Finalizing WebhookEvent status.');
    await prisma.webhookEvent.update({
      where: { id: webhookEventId },
      data: {
        processed: true,
        processedAt: new Date(),
        errorCount: 0,
        lastError: null,
      },
    });
    log.info(detailedCtx, 'Marked WebhookEvent as processed.');
    const totalProcessingTime = Date.now() - jobStartTime;
    log.info(
      detailedCtx,
      `✅ Processing completed successfully in ${totalProcessingTime}ms`
    );
    log.info(detailedCtx, `=== PERFORMANCE SUMMARY ===`);
    log.info(detailedCtx, `SMS was sent early for optimal user experience`);
  } catch (err) {
    log.error(ctx, 'Worker job failed', err);
    throw err;
  }
};

// --- Worker Initialization Function ---
let workerInstance: Worker | null = null;

export function startWorker() {
  // Prevent multiple initializations
  if (workerInstance) {
    console.warn('Worker already initialized.');
    return workerInstance;
  }

  console.log(
    `⚙️ Initializing BullMQ worker for queue: '${ORDER_PROCESSING_QUEUE_NAME}'...`
  );
  workerInstance = new Worker<OrderJobData>(
    ORDER_PROCESSING_QUEUE_NAME,
    processor,
    {
      connection: redisConfig.connection,
      concurrency: parseInt(process.env.WORKER_CONCURRENCY || '1', 10), // Set to 1 for demo
      // Use drainDelay to check for jobs more frequently (default is 5 seconds)
      drainDelay: 1, // Check for new jobs every 1ms when queue appears empty
      stalledInterval: 30000, // Check for stalled jobs every 30 seconds
    }
  );

  // --- Worker Event Listeners for Monitoring --- //
  workerInstance.on('completed', (job: Job<OrderJobData>, _result: any) => {
    const timestamp = new Date().toISOString();
    console.log(
      `[${timestamp}] ✅ [Worker Monitor | Job ${job.id}] Completed successfully.`
    );
  });

  workerInstance.on(
    'failed',
    (job: Job<OrderJobData> | undefined, err: Error) => {
      if (job) {
        const logPrefix = `[Worker Monitor | Job ${job.id} | Event ${job.data.webhookEventId}]`;
        console.error(
          `💀 ${logPrefix} Failed finally after ${job.attemptsMade} attempts. Error: ${err.message}`
        );
        prisma.webhookEvent
          .update({
            where: { id: job.data.webhookEventId },
            data: {
              processed: false,
              lastError: `FINAL FAILURE after ${job.attemptsMade} attempts: ${err.message.substring(0, 900)}`,
            },
          })
          .catch(dbError => {
            console.error(
              `${logPrefix} CRITICAL: Failed to update WebhookEvent to final failed state:`,
              dbError
            );
          });
      } else {
        console.error(
          `💀 [Worker Monitor] A job failed but the job object is undefined. Error: ${err.message}`,
          err
        );
      }
    }
  );

  workerInstance.on('error', err => {
    console.error(
      `🔴 [Worker Monitor] Worker encountered an unexpected error:`,
      err
    );
  });

  workerInstance.on('active', (job: Job<OrderJobData>) => {
    const timestamp = new Date().toISOString();
    const jobCreatedAt = new Date(job.timestamp).getTime();
    const pickupDelay = Date.now() - jobCreatedAt;
    console.log(
      `[${timestamp}] 🚀 [Worker Monitor | Job ${job.id}] Job became active (pickup delay: ${pickupDelay}ms) for Event ${job.data.webhookEventId}.`
    );
  });

  workerInstance.on('stalled', (jobId: string) => {
    console.warn(`⚠️ [Worker Monitor | Job ${jobId}] Job stalled.`);
  });

  console.log(
    `✅ BullMQ Worker for '${ORDER_PROCESSING_QUEUE_NAME}' initialized and listening for jobs.`
  );
  return workerInstance;
}

// Note: Graceful shutdown is handled by the runner script (scripts/run-worker.ts)
