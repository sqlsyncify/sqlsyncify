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
     ,pc.catId
     ,pc.catName
     ,pm.metaArrayJson
     ,c.categories
FROM  posts wp
JOIN post_meta pm ON wp.ID = pm.ID
JOIN post_cates pc ON wp.ID = pc.ID
JOIN cates c ON pc.catId = c.catId
