// =============================================================================
// MongoDB Aggregation Pipelines — E-Commerce Analytics
// Database: ecommerce
// Run against the database populated by load_data.py
// =============================================================================

use("ecommerce");

// =============================================================================
// PIPELINE 1: Product Popularity Analysis
// Business question: Which products are generating the most revenue,
// and how many units of each have been sold?
//
// Approach:
//   - Match only completed transactions (exclude processing, cancelled, etc.)
//   - Unwind the embedded items array so each line item becomes its own document
//   - Group by product_id, summing revenue (unit_price × quantity) and units sold
//   - Join with the products collection to retrieve human-readable product names
//   - Sort by total revenue descending and return the top 10
// =============================================================================

db.transactions.aggregate([

  // Stage 1: Only count completed sales
  {
    $match: { status: "completed" }
  },

  // Stage 2: Explode the items array — one document per line item
  {
    $unwind: "$items"
  },

  // Stage 3: Group by product, accumulate revenue and units sold
  {
    $group: {
      _id: "$items.product_id",
      total_revenue: {
        $sum: { $multiply: ["$items.unit_price", "$items.quantity"] }
      },
      units_sold: { $sum: "$items.quantity" },
      order_count: { $sum: 1 }
    }
  },

  // Stage 4: Join with products collection to get product names
  {
    $lookup: {
      from: "products",
      localField: "_id",
      foreignField: "product_id",
      as: "product_info"
    }
  },

  // Stage 5: Flatten the joined array (lookup returns an array)
  {
    $unwind: {
      path: "$product_info",
      preserveNullAndEmpty: false
    }
  },

  // Stage 6: Shape the final output — only expose useful fields
  {
    $project: {
      _id: 0,
      product_id: "$_id",
      product_name: "$product_info.name",
      category_id: "$product_info.category_id",
      total_revenue: { $round: ["$total_revenue", 2] },
      units_sold: 1,
      order_count: 1
    }
  },

  // Stage 7: Sort by revenue descending
  {
    $sort: { total_revenue: -1 }
  },

  // Stage 8: Return top 10 products
  {
    $limit: 10
  }

]);


// =============================================================================
// PIPELINE 2: User Segmentation by Purchase Frequency
// Business question: How do our users distribute across engagement levels,
// and what is the revenue contribution of each segment?
//
// Approach:
//   - Group transactions by user to compute order count and total spend
//   - Classify each user into a segment based on order frequency:
//       High frequency  : 5+ orders
//       Mid frequency   : 2–4 orders
//       Low frequency   : exactly 1 order
//       Inactive        : no completed transactions (not captured here —
//                         these users simply won't appear in the output)
//   - Group again by segment to produce summary statistics per tier
//   - Sort by total segment revenue descending
// =============================================================================

db.transactions.aggregate([

  // Stage 1: Only consider completed transactions
  {
    $match: { status: "completed" }
  },

  // Stage 2: Aggregate per user — order count, total spend, average order value
  {
    $group: {
      _id: "$user_id",
      order_count: { $sum: 1 },
      total_spend: { $sum: "$total" },
      avg_order_value: { $avg: "$total" }
    }
  },

  // Stage 3: Classify each user into a segment based on order frequency
  {
    $addFields: {
      segment: {
        $switch: {
          branches: [
            {
              case: { $gte: ["$order_count", 5] },
              then: "High Frequency"
            },
            {
              case: {
                $and: [
                  { $gte: ["$order_count", 2] },
                  { $lte: ["$order_count", 4] }
                ]
              },
              then: "Mid Frequency"
            },
            {
              case: { $eq: ["$order_count", 1] },
              then: "Low Frequency"
            }
          ],
          default: "Inactive"
        }
      }
    }
  },

  // Stage 4: Group by segment — compute segment-level statistics
  {
    $group: {
      _id: "$segment",
      user_count: { $sum: 1 },
      total_segment_revenue: { $sum: "$total_spend" },
      avg_spend_per_user: { $avg: "$total_spend" },
      avg_orders_per_user: { $avg: "$order_count" }
    }
  },

  // Stage 5: Shape the final output
  {
    $project: {
      _id: 0,
      segment: "$_id",
      user_count: 1,
      total_segment_revenue: { $round: ["$total_segment_revenue", 2] },
      avg_spend_per_user: { $round: ["$avg_spend_per_user", 2] },
      avg_orders_per_user: { $round: ["$avg_orders_per_user", 1] }
    }
  },

  // Stage 6: Sort by total segment revenue descending
  {
    $sort: { total_segment_revenue: -1 }
  }

]);