-- key=wp.ID
select
    wp.ID
     ,tt.term_id as cat_id
     ,t.name as cat_name
     , tt.taxonomy
FROM wp_posts AS wp
         LEFT JOIN wp_term_relationships tr ON wp.ID = tr.object_id
         LEFT JOIN wp_term_taxonomy tt ON tr.term_taxonomy_id = tt.term_taxonomy_id
         LEFT JOIN wp_terms t ON tt.term_id = t.term_id
where  wp.post_type in ('post','product','product_cat')
  and wp.post_status in ('publish')
  and tt.taxonomy in ('category', 'product_cat')
