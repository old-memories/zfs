/*
 * CDDL HEADER START
 *
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the license at usr/src/OPENSOLARIS.LICENSE
 * or http://www.opensolaris.org/os/licensing.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL HEADER in each
 * file and include the License file at usr/src/OPENSOLARIS.LICENSE.
 * If applicable, add the following below this CDDL HEADER, with the
 * fields enclosed by brackets "[]" replaced with your own identifying
 * information: Portions Copyright [yyyy] [name of copyright owner]
 *
 * CDDL HEADER END
 */

/*
 * Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved.
 * Copyright (c) 2012, 2016 by Delphix. All rights reserved.
 */

#include <sys/zfs_context.h>
#include <sys/spa.h>
#include <sys/spa_impl.h>
#include <sys/zio.h>
#include <sys/ddt.h>
#include <sys/dmu_tx.h>
#include <sys/zio_checksum.h>
#include <sys/abd.h>
#include <sys/burst_dedup.h>
#include <sys/cmn_err.h>

static kmem_cache_t *htddt_cache;
static kmem_cache_t *htddt_entry_cache;
static kmem_cache_t *bstt_cache;
static kmem_cache_t *bstt_entry_cache;



static htddt_entry_t *
htddt_alloc(const htddt_key_t *htddk)
{
	htddt_entry_t *htdde;

	htdde = kmem_cache_alloc(htddt_entry_cache, KM_SLEEP);
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====htddt_alloc: allocated htdde: %px", htdde);
	bzero(htdde, sizeof (htddt_entry_t));

	htdde->htdde_key = *htddk;

	return (htdde);
}

static void
htddt_free(htddt_entry_t *htdde)
{
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====htddt_free: freed htdde: %px", htdde);
	kmem_cache_free(htddt_entry_cache, htdde);
}

static void
htddt_remove_all(htddt_t *htddt)
{
	// ASSERT(MUTEX_HELD(&htddt->htddt_lock));
	htddt_entry_t *htdde;
	void *cookie = NULL;
	while ((htdde = avl_destroy_nodes(&htddt->htddt_tree, &cookie)) != NULL) {
		htddt_free(htdde);
	}
}


static htddt_t *
htddt_table_alloc(spa_t *spa, enum zio_checksum c, enum htddt_type type)
{
	htddt_t *htddt;

	htddt = kmem_cache_alloc(htddt_cache, KM_SLEEP);
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====htddt_table_alloc: allocated htddt: %px", htddt);
	bzero(htddt, sizeof (htddt_t));

	// mutex_init(&htddt->htddt_lock, NULL, MUTEX_DEFAULT, NULL);
	avl_create(&htddt->htddt_tree, htddt_entry_compare,
	    sizeof (htddt_entry_t), offsetof(htddt_entry_t, htdde_node));
	htddt->htddt_checksum = c;
	htddt->htddt_spa = spa;
    htddt->htddt_type = type;
	return (htddt);
}

static void
htddt_table_free(htddt_t *htddt)
{
	// mutex_destroy(&htddt->htddt_lock);
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====htddt_table_free: freed htddt: %px", htddt);
	kmem_cache_free(htddt_cache, htddt);
}

htddt_t *
htddt_select(spa_t *spa, const blkptr_t *bp, enum htddt_type type)
{
    if(type == HTDDT_TYPE_HEAD){
        return (spa->spa_hddt[BP_GET_CHECKSUM(bp)]);
    }
    if(type == HTDDT_TYPE_TAIL){
        return (spa->spa_tddt[BP_GET_CHECKSUM(bp)]);
    }
    return (NULL);
}

void 
htddt_init(void)
{
    htddt_cache = kmem_cache_create("htddt_cache",
	    sizeof (htddt_t), 0, NULL, NULL, NULL, NULL, NULL, 0);
	htddt_entry_cache = kmem_cache_create("htddt_entry_cache",
	    sizeof (htddt_entry_t), 0, NULL, NULL, NULL, NULL, NULL, 0);
}
void 
htddt_fini(void)
{
	kmem_cache_destroy(htddt_entry_cache);
	kmem_cache_destroy(htddt_cache);
}

htddt_entry_t *
htddt_lookup(htddt_t *htddt, htddt_key_t *htddk, boolean_t add, boolean_t *found)
{
	htddt_entry_t *htdde, htdde_search;
	avl_index_t where;
	
	// ASSERT(MUTEX_HELD(&htddt->htddt_lock));
	ASSERT(htddk->htddk_type == htddt->htddt_type);
	htdde_search.htdde_key.htddk_cksum = htddk->htddk_cksum;
	htdde_search.htdde_key.htddk_type =  htddk->htddk_type;
	htdde = avl_find(&htddt->htddt_tree, &htdde_search, &where);
	if (htdde == NULL) {
		if(found != NULL){
			*found = B_FALSE;
		}
		zfs_burst_dedup_dbgmsg("=====burst-dedup=====avl_find returns NULL.");
		if (!add){
			zfs_burst_dedup_dbgmsg("=====burst-dedup=====not add, just return.");
			return (NULL);
		}
		zfs_burst_dedup_dbgmsg("=====burst-dedup=====add, so alloc a new htdde and insert.");
		htdde = htddt_alloc(&htdde_search.htdde_key);
		avl_insert(&htddt->htddt_tree, htdde, where);
	}
	else{
		if(found != NULL){
			*found = B_TRUE;
		}
		zfs_burst_dedup_dbgmsg("=====burst-dedup=====avl_find returns htdde.");
	}
	return htdde;
}

void
htddt_remove(htddt_t *htddt, htddt_entry_t *htdde)
{
	// ASSERT(MUTEX_HELD(&htddt->htddt_lock));

	avl_remove(&htddt->htddt_tree, htdde);
	htddt_free(htdde);
}


void 
htddt_create(spa_t *spa)
{
    //return;
	ASSERT(spa->spa_dedup_checksum = ZIO_DEDUPCHECKSUM);
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====creating head/tail dedup tables...");
	for (enum zio_checksum c = 0; c < ZIO_CHECKSUM_FUNCTIONS; c++){
		spa->spa_hddt[c] = htddt_table_alloc(spa, c, HTDDT_TYPE_HEAD);
        spa->spa_tddt[c] = htddt_table_alloc(spa, c, HTDDT_TYPE_TAIL);
    }
}

void 
htddt_unload(spa_t *spa)
{
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====unloading head/tail dedup tables...");
	for (enum zio_checksum c = 0; c < ZIO_CHECKSUM_FUNCTIONS; c++) {
		if (spa->spa_hddt[c]) {
			// htddt_enter(spa->spa_hddt[c]);
			htddt_remove_all(spa->spa_hddt[c]);
			// htddt_exit(spa->spa_hddt[c]);
			htddt_table_free(spa->spa_hddt[c]);
			spa->spa_hddt[c] = NULL;
		}
		if (spa->spa_tddt[c]) {
			// htddt_enter(spa->spa_tddt[c]);
			htddt_remove_all(spa->spa_tddt[c]);
			// htddt_exit(spa->spa_tddt[c]);
			htddt_table_free(spa->spa_tddt[c]);
			spa->spa_tddt[c] = NULL;
		}
	}
	
}

void 
htddt_sync_table(htddt_t *htddt, ddt_t *ddt)
{
	// htddt_entry_t *htdde, *htdde_next;
	// htddt_phys_t *htddp;
	// for(htdde = avl_first(&htddt->htddt_tree); htdde != NULL; htdde = htdde_next){
	// 	htddp = &(htdde->htdde_phys);
	// 	htdde_next = AVL_NEXT(&htddt->htddt_tree, htdde);
	// 	if(!ddt_exist(ddt, htddp->htddp_dde) && htddp->valid == B_FALSE){
	// 		htddt_free(htdde);
	// 	}
	// }

}


void
htddt_bstp_fill(htddt_entry_t *htdde, bstt_phys_t *bstp)
{
	bzero(bstp, sizeof(bstt_phys_t));
	bstp->bstp_dde = htdde->htdde_phys.htddp_dde;
	bstp->bstp_dde_p = htdde->htdde_phys.htddp_dde_p;
	bstp->bstp_abd_size = htdde->htdde_phys.htddp_abd_size;
	bstp->valid = B_FALSE;
	bstp->bstp_refcnt = 1;
}


void
htddt_phys_addref(zio_t *zio, htddt_phys_t *htddp)
{
	
	htddp->htddp_refcnt++;
}

int
htddt_entry_compare(const void *x1, const void *x2)
{
	const htddt_entry_t *htdde1 = x1;
	const htddt_entry_t *htdde2 = x2;
	const htddt_key_cmp_t *k1 = (const htddt_key_cmp_t *)&htdde1->htdde_key;
	const htddt_key_cmp_t *k2 = (const htddt_key_cmp_t *)&htdde2->htdde_key;
	int32_t cmp = 0;

	for (int i = 0; i < HTDDT_KEY_CMP_LEN; i++) {
		cmp = (int32_t)k1->u16[i] - (int32_t)k2->u16[i];
		if (likely(cmp))
			break;
	}
	return (AVL_ISIGN(cmp));
}

uint64_t
htddt_htsize(uint64_t size)
{
	return P2ROUNDUP_TYPED(size >> HTDDT_HT_RIGHTSHIFT, MIN_BLOCK_SIZE, uint64_t);
}



static bstt_entry_t *
bstt_alloc(const bstt_key_t *bstk)
{
	bstt_entry_t *bste;
	bste = kmem_cache_alloc(bstt_entry_cache, KM_SLEEP);
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====bstt_alloc: allocated bste: %px", bste);
	bzero(bste, sizeof (bstt_entry_t));
	bste->bste_key = *bstk;

	return (bste);
}





static bstt_t *
bstt_table_alloc(spa_t *spa, enum zio_checksum c)
{
	bstt_t *bstt;

	bstt = kmem_cache_alloc(bstt_cache, KM_SLEEP);
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====bstt_table_alloc: allocated bstt: %px", bstt);
	bzero(bstt, sizeof (bstt_t));

	// mutex_init(&bstt->bstt_lock, NULL, MUTEX_DEFAULT, NULL);
	avl_create(&bstt->bstt_tree, bstt_entry_compare,
	    sizeof (bstt_entry_t), offsetof(bstt_entry_t, bste_node));
	bstt->bstt_checksum = c;
	bstt->bstt_spa = spa;
	return (bstt);
}

static void
bstt_table_free(bstt_t *bstt)
{
	ASSERT(avl_numnodes(&bstt->bstt_tree) == 0);
	avl_destroy(&bstt->bstt_tree);
	// mutex_destroy(&bstt->bstt_lock);
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====bstt_table_free: freed bstt: %px", bstt);
	kmem_cache_free(bstt_cache, bstt);
}



static void
bstt_free(bstt_entry_t *bste)
{
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====bstt_free: freed bste: %px", bste);
	abd_free(bste->bste_phys.bstp_burst.data);
	kmem_cache_free(bstt_entry_cache, bste);
}

static void
bstt_remove_all(bstt_t *bstt)
{
	// ASSERT(MUTEX_HELD(&bstt->bstt_lock));
	bstt_entry_t *bste;
	void *cookie = NULL;
	while ((bste = avl_destroy_nodes(&bstt->bstt_tree, &cookie)) != NULL) {
		bstt_free(bste);
	}
}


bstt_t *
bstt_select(spa_t *spa, const blkptr_t *bp)
{
	return (spa->spa_bstt[BP_GET_CHECKSUM(bp)]);
}

void 
bstt_init(void)
{
    bstt_cache = kmem_cache_create("bstt_cache",
	    sizeof (bstt_t), 0, NULL, NULL, NULL, NULL, NULL, 0);
	bstt_entry_cache = kmem_cache_create("bstt_entry_cache",
	    sizeof (bstt_entry_t), 0, NULL, NULL, NULL, NULL, NULL, 0);
}

void 
bstt_fini(void){
    kmem_cache_destroy(bstt_entry_cache);
	kmem_cache_destroy(bstt_cache);
}

bstt_entry_t *
bstt_lookup(bstt_t *bstt, bstt_key_t *bstk, boolean_t add, boolean_t *found)
{
	bstt_entry_t *bste, bste_search;
	avl_index_t where;
	
	// ASSERT(MUTEX_HELD(&bstt->bstt_lock));
	bste_search.bste_key.bstk_cksum = bstk->bstk_cksum;
	bste = avl_find(&bstt->bstt_tree, &bste_search, &where);
	if (bste == NULL) {
		if(found != NULL){
			*found = B_FALSE;
		}
		zfs_burst_dedup_dbgmsg("=====burst-dedup=====avl_find returns NULL.");
		if (!add){
			zfs_burst_dedup_dbgmsg("=====burst-dedup=====not add, just return.");
			return (NULL);
		}
		zfs_burst_dedup_dbgmsg("=====burst-dedup=====add, so alloc a new bste and insert.");
		bste = bstt_alloc(&bste_search.bste_key);
		avl_insert(&bstt->bstt_tree, bste, where);
	}
	else{
		if(found != NULL){
			*found = B_TRUE;
		}
		zfs_burst_dedup_dbgmsg("=====burst-dedup=====avl_find returns bste.");
	}
	return bste;
}

void 
bstt_remove(bstt_t *bstt, bstt_entry_t *bste)
{
	avl_remove(&bstt->bstt_tree, bste);
	bstt_free(bste);
}

void 
bstt_create(spa_t *spa)
{
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====creating burst tables...");
	ASSERT(spa->spa_dedup_checksum = ZIO_DEDUPCHECKSUM);
    for (enum zio_checksum c = 0; c < ZIO_CHECKSUM_FUNCTIONS; c++){
		spa->spa_bstt[c] = bstt_table_alloc(spa, c);
    }
}



void 
bstt_unload(spa_t *spa)
{
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====unloading burst tables...");
	for (enum zio_checksum c = 0; c < ZIO_CHECKSUM_FUNCTIONS; c++) {
		if (spa->spa_bstt[c]) {
			bstt_remove_all(spa->spa_bstt[c]);
			bstt_table_free(spa->spa_bstt[c]);
			spa->spa_bstt[c] = NULL;
		}
	}
}

void 
bstt_sync_table(bstt_t *bstt, ddt_t *ddt, uint64_t txg)
{
	bstt_entry_t *bste, *bste_next;
	bstt_phys_t *bstp;
	for(bste = avl_first(&bstt->bstt_tree); bste != NULL; bste = bste_next){
		bstp = &(bste->bste_phys);
		bste_next = AVL_NEXT(&bstt->bstt_tree, bste);
		if(bstp->bstp_refcnt != 0)
			continue;
		if(!ddt_exist(ddt, bstp->bstp_dde) && bstp->valid == B_FALSE){
			blkptr_t blk;
			
			BP_ZERO(&blk);
			blk.blk_cksum = bste->bste_key.bstk_cksum;
			BP_SET_LSIZE(&blk, BSTP_GET_LSIZE(bstp));
			BP_SET_PSIZE(&blk, BSTP_GET_PSIZE(bstp));
			BP_SET_COMPRESS(&blk, BSTP_GET_COMPRESS(bstp));
			BP_SET_CRYPT(&blk, BSTP_GET_CRYPT(bstp));
			BP_SET_FILL(&blk, 1);
			BP_SET_CHECKSUM(&blk, bstt->bstt_checksum);
			BP_SET_TYPE(&blk, DMU_OT_DEDUP);
			BP_SET_LEVEL(&blk, 0);
			BP_SET_DEDUP(&blk, 0);
			BP_SET_BYTEORDER(&blk, ZFS_HOST_BYTEORDER);
			zio_free(bstt->bstt_spa, txg, &blk);
			bstt_free(bste);
		}
	}
}

int
bstt_entry_compare(const void *x1, const void *x2)
{
	const bstt_entry_t *bste1 = x1;
	const bstt_entry_t *bste2 = x2;
	const bstt_key_cmp_t *k1 = (const bstt_key_cmp_t *)&bste1->bste_key;
	const bstt_key_cmp_t *k2 = (const bstt_key_cmp_t *)&bste2->bste_key;
	int32_t cmp = 0;

	for (int i = 0; i < BSTT_KEY_CMP_LEN; i++) {
		cmp = (int32_t)k1->u16[i] - (int32_t)k2->u16[i];
		if (likely(cmp))
			break;
	}
	return (AVL_ISIGN(cmp));
}


void 
bstt_bstp_fill(bstt_phys_t *bstp, blkptr_t *bp)
{
	ASSERT(bstp->bstp_phys_birth == 0);

	bstp->bstp_prop = 0;
	BSTP_SET_LSIZE(bstp, BP_GET_LSIZE(bp));
	BSTP_SET_PSIZE(bstp, BP_GET_PSIZE(bp));
	BSTP_SET_COMPRESS(bstp, BP_GET_COMPRESS(bp));
	BSTP_SET_CRYPT(bstp, BP_USES_CRYPT(bp));

	for (int d = 0; d < SPA_DVAS_PER_BP; d++)
		bstp->bstp_burst_dva[d] = bp->blk_dva[d];
	bstp->bstp_phys_birth = BP_PHYSICAL_BIRTH(bp);
}

void
bstt_bp_fill(bstt_phys_t *bstp, blkptr_t *bp, uint64_t txg)
{
	ASSERT(txg != 0);
	/* TODO: set virtual address for bp from ddp */
	for (int d = 0; d < SPA_DVAS_PER_BP; d++)
		bp->blk_dva[d] = bstp->bstp_burst_dva[d];
	BP_SET_BIRTH(bp, txg, bstp->bstp_phys_birth);
}


void
bstt_phys_addref(zio_t *zio, bstt_phys_t *bstp)
{
	uint8_t p = bstp->bstp_dde_p;
	ddt_entry_t *dde = bstp->bstp_dde;
	bstp->bstp_refcnt++;
	if(dde->dde_lead_zio[p] != NULL){
		zfs_burst_dedup_dbgmsg("=====burst-dedup=====dde->dde_lead_zio[p] != NULL, zio: %px", zio);
		zio_add_child(zio, dde->dde_lead_zio[p]);
	}
	else{
		zfs_burst_dedup_dbgmsg("=====burst-dedup=====before refcnt +1. refcnt: %llu, zio: %px", dde->dde_phys[p].ddp_refcnt, zio);
		ddt_phys_addref(&(dde->dde_phys[p]));
		zfs_burst_dedup_dbgmsg("=====burst-dedup=====after refcnt +1. refcnt: %llu, zio: %px", dde->dde_phys[p].ddp_refcnt, zio);
	}
}

void
bstt_create_burst(burst_t *burst, abd_t *based_data, uint64_t based_data_size, abd_t *new_data, uint64_t new_data_size)
{
	unsigned char *based_char, *new_char;
    int pos;
	uint64_t abd_size;

	based_char = abd_borrow_buf_copy(based_data, based_data_size);
	new_char = abd_borrow_buf_copy(new_data, new_data_size);

    pos = 0;
    while(pos < based_data_size && pos < new_data_size){
        if(*new_char == *based_char){
           new_char++;
           based_char++;
           pos++;
        }
        else{
            break;
        }
    }
    burst->start_pos = pos;

    based_char -= pos;
    based_char += based_data_size - 1;
    new_char -= pos;
    new_char += new_data_size - 1;
    pos = 0;
    while(pos < based_data_size && pos < new_data_size){
        if(*new_char == *based_char){
           pos++;
           new_char--;
           based_char--;
        }
        else{
            break;
        }
    }
	based_char += pos;
	based_char -= based_data_size - 1;
	new_char += pos;
	new_char -= new_data_size - 1;
	abd_return_buf(new_data, new_char, new_data_size);
	abd_return_buf(based_data, based_char, based_data_size);

    burst->end_pos = based_data_size - pos -1;
    burst->length = new_data_size - pos  - burst->start_pos;
    
	abd_size = burst->length > SPA_MINBLOCKSIZE ? P2ROUNDUP_TYPED(burst->length, SPA_MINBLOCKSIZE, uint64_t) : SPA_MINBLOCKSIZE;
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====burst: start_pos: %d, end_pos: %d, length: %lu, roundup to: %llu", burst->start_pos, burst->end_pos, burst->length, abd_size);
	burst->data = abd_alloc(abd_size, B_FALSE);
	if(burst->length > 0){
        abd_copy_off(burst->data, new_data, 0, (size_t)burst->start_pos, burst->length);
    }
    else{
        burst->length = 0;
		// dummy value
		unsigned char * burst_data_buf = abd_borrow_buf_copy(burst->data, 1);
		memset(burst_data_buf, 0, 1);
		abd_return_buf_copy(burst->data, burst_data_buf, 1);
    }
	//  zfs_dbgmsg("burst: start_pos: %d, end_pos: %d, length: %lu", burst->start_pos, burst->end_pos, burst->length);

}

void
bstt_create_data(burst_t *burst, abd_t *based_data, uint64_t based_data_size, abd_t *new_data, uint64_t new_data_size)
{
	
	abd_copy_off(new_data, based_data, 0, 0, (size_t)burst->start_pos);
	abd_copy_off(new_data, burst->data, (size_t)burst->start_pos, 0, burst->length);
	abd_copy_off(new_data, based_data, (size_t)burst->start_pos + burst->length, burst->end_pos + 1, based_data_size - (size_t)burst->end_pos - 1);
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====burst: start_pos: %d, end_pos: %d, length: %lu, roundup to: %llu", burst->start_pos, burst->end_pos, burst->length, burst->data->abd_size);

}
