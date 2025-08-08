// @ts-ignore
import prisma from '../../db.server.js';

/**
 * Service for managing digital ID creation, minting, and ownership.
 */
export class DigitalIdService {
  /**
   * Create a digital ID for a product variant purchased in an order.
   * This should be called during order processing.
   */
  async createDigitalIdForOrderItem({
    shopId, // This parameter contains the myshopify domain
    coreProductVariantId,
    shopifyOrderId,
    shopifyLineItemId,
    owner3faId,
    orders3faId,
  }: {
    shopId: string;
    coreProductVariantId: string;
    shopifyOrderId: string;
    shopifyLineItemId: string;
    owner3faId: string;
    orders3faId: string;
  }) {
    console.log(
      `Creating Digital ID for order ${shopifyOrderId}, line item ${shopifyLineItemId}`
    );

    // Check if a digital ID already exists for this line item to prevent duplicates
    // @ts-ignore
    const existingDigitalId = await prisma.uniqueDigitalId.findUnique({
      where: { shopifyLineItemId },
    });

    if (existingDigitalId) {
      console.log(
        `Digital ID already exists for line item ${shopifyLineItemId}, ID: ${existingDigitalId.id}`
      );
      return existingDigitalId;
    }

    // Get the core product variant to retrieve default contract address and brand relationship
    // @ts-ignore
    const coreVariant = await prisma.coreProductVariant.findUnique({
      where: { id: coreProductVariantId },
    });

    if (!coreVariant) {
      throw new Error(
        `Core product variant not found: ${coreProductVariantId}`
      );
    }

    // @ts-ignore - Access the brand_3fa_id field regardless of its actual name in Prisma client
    if (!coreVariant.brand_3fa_id) {
      throw new Error(
        `Core product variant ${coreProductVariantId} is missing brand_3fa_id`
      );
    }

    // Create the digital ID with full @ts-ignore to bypass type checking
    // @ts-ignore
    const digitalId = await prisma.uniqueDigitalId.create({
      data: {
        coreProductVariantId,
        // Based on errors, we should only use these field names
        myshopify_domain: shopId,
        brand_3fa_id: coreVariant.brand_3fa_id,
        shopifyOrderId,
        shopifyLineItemId,
        owner3faId,
        orders3faId,
        status: 'CREATED', // Initial status
        contractAddress:
          coreVariant.defaultSmartContractAddress ||
          process.env.DEFAULT_CONTRACT_ADDRESS,
      },
    });

    console.log(
      `Created Digital ID ${digitalId.id} for product variant ${coreProductVariantId}`
    );

    // Queue token minting (to be implemented)
    await this.queueTokenMinting(digitalId.id);

    return digitalId;
  }

  /**
   * Create all digital IDs for a shop's order.
   * Creates one digital ID per line item quantity.
   */
  async createDigitalIdsForOrder({
    shopId, // This parameter contains the myshopify domain
    shopifyOrderId,
    lineItems,
    owner3faId,
    privyWalletAddress,
    privyDid,
    orders3faId,
  }: {
    shopId: string;
    shopifyOrderId: string;
    lineItems: Array<{
      id: string; // Line item ID in our database
      shopifyId: bigint; // Shopify line item ID
      variantId?: bigint | null; // Shopify variant ID
      quantity: number;
    }>;
    owner3faId: string;
    privyWalletAddress?: string | null;
    privyDid?: string | null;
    orders3faId: string; // Add new parameter for Orders3fa ID
  }) {
    console.log(
      `Creating Digital IDs for order ${shopifyOrderId}, with ${lineItems.length} line items. Privy DID: ${privyDid}`
    );

    const results = {
      created: 0,
      failed: 0,
      digitalIds: [] as any[],
    };

    for (const lineItem of lineItems) {
      try {
        if (!lineItem.variantId) {
          console.log(`Skipping line item ${lineItem.id} - no variant ID`);
          continue;
        }

        // Find the CoreProductVariant based on Shopify variant GID
        const shopifyVariantGid = `gid://shopify/ProductVariant/${lineItem.variantId}`;
        // @ts-ignore - Full ignore to bypass type checking issues
        const coreVariant = await prisma.coreProductVariant.findFirst({
          where: {
            shopifyVariantGid,
            // @ts-ignore - Use myshopify_domain as that's what appears in TS definitions
            myshopify_domain: shopId,
          },
        });

        if (!coreVariant) {
          console.warn(`No CoreProductVariant found for ${shopifyVariantGid}`);
          results.failed += lineItem.quantity;
          continue;
        }

        // @ts-ignore - Access the brand_3fa_id field regardless of its actual name in Prisma client
        if (!coreVariant.brand_3fa_id) {
          console.warn(
            `CoreProductVariant for ${shopifyVariantGid} is missing brand_3fa_id`
          );
          results.failed += lineItem.quantity;
          continue;
        }

        // Create a Digital ID for each quantity of the line item
        for (let i = 0; i < lineItem.quantity; i++) {
          const shopifyLineItemGid = `gid://shopify/LineItem/${lineItem.shopifyId}`;

          // For multiple quantity, append the index to make the shopifyLineItemId unique
          const uniqueLineItemId =
            lineItem.quantity > 1
              ? `${shopifyLineItemGid}:${i + 1}`
              : shopifyLineItemGid;

          // @ts-ignore - Full ignore to bypass type checking issues
          const digitalId = await prisma.uniqueDigitalId.create({
            data: {
              coreProductVariantId: coreVariant.id,
              // Based on errors, we should only use these field names
              myshopify_domain: shopId,
              brand_3fa_id: coreVariant.brand_3fa_id,
              shopifyOrderId: `gid://shopify/Order/${shopifyOrderId}`,
              shopifyLineItemId: uniqueLineItemId,
              owner3faId,
              orders3faId, // Add the orders3faId to the create payload
              privyWalletAddress: privyWalletAddress,
              privyDid: privyDid,
              status: 'CREATED', // Initial status
              contractAddress:
                coreVariant.defaultSmartContractAddress ||
                process.env.DEFAULT_CONTRACT_ADDRESS,
            },
          });

          // Queue token minting (to be implemented)
          await this.queueTokenMinting(digitalId.id);

          console.log(
            `Created Digital ID ${digitalId.id} for variant ${coreVariant.id}`
          );
          results.created++;
          results.digitalIds.push(digitalId);
        }
      } catch (error) {
        console.error(
          `Error creating Digital ID for line item ${lineItem.id}: ${error instanceof Error ? error.message : 'Unknown error'}`
        );
        results.failed += lineItem.quantity;
      }
    }

    return results;
  }

  /**
   * Queue the minting of a token for a digital ID
   * This integrates with the actual minting service
   */
  private async queueTokenMinting(digitalIdId: string) {
    console.log(`[Mint] Preparing token mint for Digital ID ${digitalIdId}`);

    // Set pending status first
    // @ts-ignore
    await prisma.uniqueDigitalId.update({
      where: { id: digitalIdId },
      data: { status: 'MINT_PENDING' },
    });

    try {
      // Check minimal env presence before attempting mint and importing thirdweb
      if (!process.env.BASE_MINTER_PRIVATE_KEY || !process.env.THIRDWEB_CLIENT_ID || !process.env.APP_BASE_URL) {
        console.warn('[Mint] Missing env for on-chain mint. Skipping actual mint.');
        return;
      }

      // Lazy import to avoid loading thirdweb on environments/tests that don't need it
      const { mintDigitalIdOnBase } = await import(
        '../token-minting/token-minting.service.js'
      );
      const result = await mintDigitalIdOnBase(digitalIdId);
      
      if (!result.success) {
        console.error('[Mint] Mint failed:', result.error);
      } else {
        console.log('[Mint] Mint succeeded. Tx:', result.transactionHash);
      }
    } catch (err) {
      console.error('[Mint] Error during minting step:', err);
      // Leave status as MINT_PENDING or set to MINT_FAILED depending on policy
    }
  }

  /**
   * Update the status of a digital ID
   */
  async updateDigitalIdStatus(digitalIdId: string, status: string) {
    // @ts-ignore
    return prisma.uniqueDigitalId.update({
      where: { id: digitalIdId },
      data: { status },
    });
  }

  /**
   * Mark a digital ID as minted with blockchain details
   */
  async markAsMinted(
    digitalIdId: string,
    {
      tokenId,
      transactionHash,
      blockchain,
    }: {
      tokenId: string;
      transactionHash: string;
      blockchain: string;
    }
  ) {
    // @ts-ignore
    return prisma.uniqueDigitalId.update({
      where: { id: digitalIdId },
      data: {
        status: 'MINTED',
        tokenId,
        transactionHash,
        blockchain,
        mintedAt: new Date(),
      },
    });
  }

  /**
   * Get all digital IDs for a specific order
   */
  async getDigitalIdsForOrder(shopifyOrderId: string) {
    // @ts-ignore
    return prisma.uniqueDigitalId.findMany({
      where: { shopifyOrderId },
      include: {
        coreProductVariant: true,
        owner: true,
      },
      orderBy: { createdAt: 'desc' },
    });
  }

  /**
   * Get a digital ID by ID
   */
  async getDigitalId(id: string) {
    // @ts-ignore
    return prisma.uniqueDigitalId.findUnique({
      where: { id },
      include: {
        coreProductVariant: true,
        owner: true,
      },
    });
  }
}

// Export a singleton instance
export const digitalIdService = new DigitalIdService();
