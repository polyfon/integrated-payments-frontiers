# 3FA MVP Hackathon: Checkout-to-Minting Flow

This document provides a focused overview of the core services responsible for the checkout-to-minting flow in the 3FA MVP project. It is intended for hackathon participants who need to quickly understand the key components of the system.

## 🏗️ System Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Shopify       │    │   Database       │    │   Consumer      │
│   Store         │──▶ │   (PostgreSQL)   │ ──▶│   App           │
│                 │    │   + Redis        │    │   (Next.js)     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Shopify App   │    │   Background     │    │   Blockchain    │
│   (Remix)       │    │   Workers        │    │   (Base)        │
│                 │    │   (BullMQ)       │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

**Data Flow:**

1.  A new order in Shopify triggers the `orders-create` webhook.
2.  The webhook enqueues a job for the `orderProcessor` worker.
3.  The `orderProcessor` worker calls the `digital-id.service` to create unique digital IDs for each item in the order.
4.  The `digital-id.service` then calls the `token-minting.service` to mint a new token on the Base blockchain.
5.  The consumer app allows users to view their digital products, which are represented by the minted tokens.

## 🔄 System Workflows

### Token Transfer at Checkout

The core of this hackathon project is the automated process that transforms a physical product purchase into a digital asset on the blockchain. This is orchestrated by a series of services that are triggered by a new order in Shopify.

1.  **Webhook Trigger**: The process begins when a new order is created in a Shopify store. This event triggers a webhook that sends a POST request to the `/webhooks/app/orders-create` endpoint in the Shopify app.

2.  **Order Processing**: The `orders-create` webhook receives the order data and enqueues a job for the `orderProcessor.worker`. This worker is responsible for handling the entire order processing workflow in the background, ensuring that the checkout process is not blocked.

3.  **Digital ID Creation**: The `orderProcessor.worker` calls the `digital-id.service.server` to create a unique digital ID for each item in the order. This service is responsible for generating a unique identifier that links the physical product to its digital counterpart.

4.  **Token Minting**: Once the digital ID is created, the `digital-id.service.server` calls the `token-minting.service` to mint a new token on the Base blockchain. This service interacts with the `thirdweb` library to create a new NFT that represents the digital ownership of the product.

5.  **Digital Product Ownership**: The minted token is then associated with the user's wallet address, which is captured during the checkout process. The user can then view their digital products in the consumer app, which verifies ownership by checking the blockchain.

### Background Jobs

- **Order Processing**: Creates digital IDs and processes blockchain minting
- **Inventory Sync**: Synchronizes product data from Shopify
- **Media Processing**: Downloads and processes product images
- **Webhook Processing**: Handles incoming Shopify webhooks

## 🔌 API Integrations

### External Services Required

| Service      | Purpose                   | Environment Variables                          |
| ------------ | ------------------------- | ---------------------------------------------- |
| **Supabase** | Database & Storage        | `DATABASE_URL`, `SUPABASE_URL`, `SUPABASE_KEY` |
| **Shopify**  | E-commerce Platform       | `SHOPIFY_API_KEY`, `SHOPIFY_API_SECRET`        |
| **Privy**    | Web3 Authentication       | `PRIVY_API_ID`, `PRIVY_APP_SECRET`             |
| **Twilio**   | SMS Verification          | `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`      |
| **Thirdweb** | Blockchain Infrastructure | `THIRDWEB_CLIENT_ID`, `THIRDWEB_SECRET_KEY`    |
| **Redis**    | Job Queue & Caching       | `REDIS_URL`                                    |

### API Endpoints

#### Consumer App APIs

```
/api/auth/*                    # NextAuth & Privy authentication
/api/brands                    # Brand data and filtering
/api/products                  # Product catalog and details
/api/orders                    # Order history and tracking
/api/protected/user           # User profile data
```

#### Shopify App Webhooks

```
/webhooks/app/orders-create   # Process new orders
/webhooks/app/uninstalled     # Handle app uninstallation
/webhooks/changelog           # Product/inventory updates
``` 