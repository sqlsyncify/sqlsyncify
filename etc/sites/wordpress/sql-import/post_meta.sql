SET group_concat_max_len = 10485760;  -- 10MB

select
    wp.ID
     ,CONCAT(
        '[',
        GROUP_CONCAT(
                JSON_OBJECT(
                        'meta_key', pm.meta_key,
                        'meta_value', pm.meta_value
                )
        ),
        ']'
      ) as metaArrayJson
FROM wp_posts AS wp
         LEFT JOIN wp_postmeta pm ON wp.ID = pm.post_id
where 1=1
  and wp.post_type in ('post','product')
  and wp.post_status in ('publish')
GROUP BY wp.ID
