-- key=wp.ID
SET group_concat_max_len = 10485760;  -- 10MB
select
    wp.ID
     ,CONCAT(
        '[',
        GROUP_CONCAT(
                JSON_OBJECT(
                        pm.meta_key,
                        pm.meta_value
                )
        ),
        ']'
      ) as metaArrayJson
FROM wp_posts AS wp
    JOIN wp_postmeta pm ON wp.ID = pm.post_id
where 1=1
  and wp.post_type in ('post','product')
  and wp.post_status in ('publish')
  and (pm.meta_value is not null or LENGTH(pm.meta_value) > 0)
GROUP BY wp.ID
