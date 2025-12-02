SELECT
    wp.ID
     ,wp.post_type
     ,wp.post_date
     ,wp.post_date_gmt
     ,wp.post_title
     ,wp.post_excerpt
     ,wp.post_name
     ,wp.post_status
     ,wp.post_modified
     ,wp.post_modified_gmt
     ,wp.guid
FROM wp_posts wp
where  wp.post_type in ('post','product')
  and wp.post_status in ('publish')
