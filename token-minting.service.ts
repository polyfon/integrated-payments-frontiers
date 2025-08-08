import { createThirdwebClient, getContract, prepareContractCall, sendTransaction } from "thirdweb";
import { privateKeyToAccount } from "thirdweb/wallets";
import { base, baseSepolia } from "thirdweb/chains";
// @ts-ignore
import prisma from "../../db.server.js";

function getChain() {
  const chainId = process.env.BASE_CHAIN_ID?.trim();
  return chainId === "8453" ? base : baseSepolia;
}

function getBlockchainName(): string {
  const chainId = process.env.BASE_CHAIN_ID?.trim();
  return chainId === "8453" ? "base" : "base-sepolia";
}

function ensureEnv(): { clientId: string; privateKey: string; appBaseUrl: string } {
  const clientId = process.env.THIRDWEB_CLIENT_ID;
  const privateKey = process.env.BASE_MINTER_PRIVATE_KEY;
  const appBaseUrl = process.env.APP_BASE_URL;

  if (!clientId) throw new Error("THIRDWEB_CLIENT_ID is required for minting");
  if (!privateKey) throw new Error("BASE_MINTER_PRIVATE_KEY is required for minting");
  if (!appBaseUrl) throw new Error("APP_BASE_URL is required to build token metadata URLs");

  // Validate and normalize the private key
  let pk = privateKey.trim();
  if (pk.startsWith('0x')) {
    pk = pk.substring(2);
  }

  if (!/^[0-9a-fA-F]{64}$/.test(pk)) {
    throw new Error(
      `Invalid private key format. Expected a 64-character hex string, but got a key with length ${pk.length} after trimming.`
    );
  }

  const normalizedPrivateKey = `0x${pk}`;

  return { clientId, privateKey: normalizedPrivateKey, appBaseUrl };
}

async function tryMintWithSignature(contract: any, account: any, method: string, params: any[]) {
  const tx = prepareContractCall({ contract, method, params });
  const receipt = await sendTransaction({ transaction: tx, account });
  return receipt;
}

export async function mintDigitalIdOnBase(digitalIdId: string): Promise<{
  success: boolean;
  transactionHash?: string;
  tokenId?: string;
  error?: string;
}> {
  console.log(`[Mint] Starting mint process for digital ID: ${digitalIdId}`);
  
  try {
    const { clientId, privateKey, appBaseUrl } = ensureEnv();

    // Load the Digital ID and related data
    // @ts-ignore
    const digitalId = await prisma.uniqueDigitalId.findUnique({
      where: { id: digitalIdId },
      include: {
        coreProductVariant: {
          select: {
            id: true,
            defaultSmartContractAddress: true,
          },
        },
      },
    });

    if (!digitalId) {
      throw new Error(`UniqueDigitalId not found: ${digitalIdId}`);
    }

    console.log(`[Mint] Found digital ID, checking recipient wallet...`);
    const recipient: string | undefined = digitalId.privyWalletAddress || undefined;
    if (!recipient) {
      throw new Error("Recipient wallet address (privyWalletAddress) is missing");
    }
    
    console.log(`[Mint] Recipient: ${recipient}`);

    const contractAddress: string | undefined =
      digitalId.contractAddress ||
      digitalId.coreProductVariant?.defaultSmartContractAddress ||
      process.env.DEFAULT_CONTRACT_ADDRESS ||
      undefined;

    if (!contractAddress) {
      throw new Error("No contract address found for minting (set default or per-variant)");
    }

    console.log(`[Mint] Using contract: ${contractAddress}`);
    const metadataUrl = `${appBaseUrl.replace(/\/$/, "")}/api/token-metadata/${digitalIdId}`;
    console.log(`[Mint] Metadata URL: ${metadataUrl}`);

    // Initialize thirdweb client and account
    console.log(`[Mint] Initializing thirdweb client...`);
    const client = createThirdwebClient({ clientId });
    const account = privateKeyToAccount({ client, privateKey });
    const chain = getChain();
    console.log(`[Mint] Using blockchain: ${getBlockchainName()} (${chain.id})`);
    const contract = getContract({ client, chain, address: contractAddress });

    // Try common mint method names in order
    console.log(`[Mint] Attempting to mint token...`);
    let receipt: any | undefined;
    let lastError: any | undefined;

    const mintMethods = [
      "function mintTo(address to, string uri) returns (uint256 tokenId)",
      "function safeMint(address to, string uri)",
    ];

    for (const method of mintMethods) {
      try {
        console.log(`[Mint] Trying method: ${method.split('function ')[1]}`);
        receipt = await tryMintWithSignature(contract, account, method, [recipient, metadataUrl]);
        lastError = undefined;
        console.log(`[Mint] Method succeeded!`);
        break;
      } catch (err) {
        console.log(`[Mint] Method failed: ${err instanceof Error ? err.message : String(err)}`);
        lastError = err;
      }
    }

    if (!receipt) {
      throw new Error(
        `Mint transaction failed: ${lastError instanceof Error ? lastError.message : String(lastError)}`
      );
    }

    const transactionHash: string | undefined = receipt?.transactionHash;
    console.log(`[Mint] Transaction hash: ${transactionHash}`);

    // Update the record as minted. tokenId parsing is contract-specific; omit if not easily derivable.
    // @ts-ignore
    await prisma.uniqueDigitalId.update({
      where: { id: digitalIdId },
      data: {
        status: "MINTED",
        blockchain: getBlockchainName(),
        transactionHash: transactionHash,
        contractAddress: contractAddress,
        mintedAt: new Date(),
      },
    });

    console.log(`[Mint] Successfully minted and updated database for digital ID: ${digitalIdId}`);
    return { success: true, transactionHash };
  } catch (error: any) {
    console.error(`[Mint] Error during minting for digital ID ${digitalIdId}:`, error);
    
    // Mark as failed to allow retries/visibility
    try {
      // @ts-ignore
      await prisma.uniqueDigitalId.update({
        where: { id: digitalIdId },
        data: { status: "MINT_FAILED" },
      });
      console.log(`[Mint] Marked digital ID ${digitalIdId} as MINT_FAILED`);
    } catch (dbError) {
      console.error(`[Mint] Failed to update database status for ${digitalIdId}:`, dbError);
    }

    return { success: false, error: error?.message || String(error) };
  }
}