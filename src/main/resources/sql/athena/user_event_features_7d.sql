WITH params AS (
    SELECT DATE('%s') AS snapshot_date
),
base AS (
    SELECT
        p.snapshot_date,
        r.member_id,
        r.event_name,
        CAST(SUBSTR(r."timestamp", 1, 10) AS DATE) AS event_date,
        UPPER(r.event_properties.product_type) AS product_type,
        r.event_properties.tags AS tags
    FROM holliverse_analytics.customer_event_logs_raw r
    CROSS JOIN params p
    WHERE CAST(r.dt AS DATE)
          BETWEEN date_add('day', -89, p.snapshot_date) AND p.snapshot_date
),
counts AS (
    SELECT
        snapshot_date,
        member_id,
        SUM(CASE WHEN event_name = 'click_list_type'
                  AND event_date BETWEEN date_add('day', -6, snapshot_date) AND snapshot_date
                 THEN 1 ELSE 0 END) AS click_list_type_cnt,
        SUM(CASE WHEN event_name = 'click_product_detail'
                  AND event_date BETWEEN date_add('day', -6, snapshot_date) AND snapshot_date
                 THEN 1 ELSE 0 END) AS click_product_detail_cnt,
        SUM(CASE WHEN event_name = 'click_compare'
                  AND event_date BETWEEN date_add('day', -6, snapshot_date) AND snapshot_date
                 THEN 1 ELSE 0 END) AS click_compare_cnt,
        SUM(CASE WHEN event_name = 'click_coupon'
                  AND event_date BETWEEN date_add('day', -6, snapshot_date) AND snapshot_date
                 THEN 1 ELSE 0 END) AS click_coupon_cnt,
        SUM(CASE WHEN event_name = 'click_penalty'
                  AND event_date BETWEEN date_add('day', -6, snapshot_date) AND snapshot_date
                 THEN 1 ELSE 0 END) AS click_penalty_cnt,
        SUM(CASE WHEN event_name = 'click_change'
                  AND event_date BETWEEN date_add('day', -6, snapshot_date) AND snapshot_date
                 THEN 1 ELSE 0 END) AS click_change_cnt,
        SUM(CASE WHEN event_name = 'click_change_success'
                  AND event_date BETWEEN date_add('day', -89, snapshot_date) AND snapshot_date
                 THEN 1 ELSE 0 END) AS click_change_success_cnt
    FROM base
    GROUP BY 1, 2
),
product_type_counts AS (
    SELECT
        snapshot_date,
        member_id,
        json_format(CAST(map_agg(product_type, cnt) AS JSON)) AS product_type_clicks
    FROM (
        SELECT
            snapshot_date,
            member_id,
            product_type,
            COUNT(*) AS cnt
        FROM base
        WHERE product_type IS NOT NULL
          AND event_date BETWEEN date_add('day', -6, snapshot_date) AND snapshot_date
        GROUP BY 1, 2, 3
    ) t
    GROUP BY 1, 2
),
product_type_tag_counts AS (
    SELECT
        b.snapshot_date,
        b.member_id,
        tag,
        COUNT(*) AS tag_cnt
    FROM base b
    CROSS JOIN UNNEST(b.tags) AS u(tag)
    WHERE b.event_name = 'click_product_detail'
      AND b.tags IS NOT NULL
      AND b.event_date BETWEEN date_add('day', -6, b.snapshot_date) AND b.snapshot_date
    GROUP BY 1, 2, 3
),
ranked_product_type_tags AS (
    SELECT
        snapshot_date,
        member_id,
        tag,
        tag_cnt,
        row_number() OVER (
            PARTITION BY snapshot_date, member_id
            ORDER BY tag_cnt DESC, tag ASC
        ) AS rn
    FROM product_type_tag_counts
),
product_type_top_tags AS (
    SELECT
        snapshot_date,
        member_id,
        json_format(CAST(array_agg(tag ORDER BY tag_cnt DESC, tag ASC) AS JSON)) AS product_type_top_tags
    FROM ranked_product_type_tags
    WHERE rn <= 3
    GROUP BY 1, 2
)
SELECT
    c.snapshot_date,
    c.member_id,
    c.click_list_type_cnt,
    c.click_product_detail_cnt,
    c.click_compare_cnt,
    c.click_coupon_cnt,
    c.click_penalty_cnt,
    c.click_change_cnt,
    c.click_change_success_cnt,
    COALESCE(ptc.product_type_clicks, '{}') AS product_type_clicks,
    COALESCE(ptt.product_type_top_tags, '[]') AS product_type_top_tags
FROM counts c
LEFT JOIN product_type_counts ptc
    ON c.snapshot_date = ptc.snapshot_date
   AND c.member_id = ptc.member_id
LEFT JOIN product_type_top_tags ptt
    ON c.snapshot_date = ptt.snapshot_date
   AND c.member_id = ptt.member_id
ORDER BY c.member_id
