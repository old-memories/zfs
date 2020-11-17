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
#include <sys/zap.h>
#include <sys/dmu_tx.h>
#include <sys/arc.h>
#include <sys/dsl_pool.h>
#include <sys/zio_checksum.h>
#include <sys/zio_compress.h>
#include <sys/dsl_scan.h>
#include <sys/abd.h>

static kmem_cache_t *ddt_cache;
static kmem_cache_t *ddt_entry_cache;

/*
 * Enable/disable prefetching of dedup-ed blocks which are going to be freed.
 */
int zfs_dedup_prefetch = 0;

static const ddt_ops_t *ddt_ops[DDT_TYPES] = {
	&ddt_zap_ops,
};

static const char *ddt_class_name[DDT_CLASSES] = {
	"ditto",
	"duplicate",
	"unique",
};

// static void
// ddt_object_create(ddt_t *ddt, enum ddt_type type, enum ddt_class class,
//     dmu_tx_t *tx)
// {
// 	spa_t *spa = ddt->ddt_spa;
// 	objset_t *os = ddt->ddt_os;
// 	uint64_t *objectp = &ddt->ddt_object[type][class];
// 	boolean_t prehash = zio_checksum_table[ddt->ddt_checksum].ci_flags &
// 	    ZCHECKSUM_FLAG_DEDUP;
// 	char name[DDT_NAMELEN];

// 	ddt_object_name(ddt, type, class, name);

// 	ASSERT(*objectp == 0);
// 	VERIFY(ddt_ops[type]->ddt_op_create(os, objectp, tx, prehash) == 0);
// 	ASSERT(*objectp != 0);

// 	VERIFY(zap_add(os, DMU_POOL_DIRECTORY_OBJECT, name,
// 	    sizeof (uint64_t), 1, objectp, tx) == 0);

// 	VERIFY(zap_add(os, spa->spa_ddt_stat_object, name,
// 	    sizeof (uint64_t), sizeof (ddt_histogram_t) / sizeof (uint64_t),
// 	    &ddt->ddt_histogram[type][class], tx) == 0);
// }

// static void
// ddt_object_destroy(ddt_t *ddt, enum ddt_type type, enum ddt_class class,
//     dmu_tx_t *tx)
// {
// 	spa_t *spa = ddt->ddt_spa;
// 	objset_t *os = ddt->ddt_os;
// 	uint64_t *objectp = &ddt->ddt_object[type][class];
// 	uint64_t count;
// 	char name[DDT_NAMELEN];

// 	ddt_object_name(ddt, type, class, name);

// 	ASSERT(*objectp != 0);
// 	ASSERT(ddt_histogram_empty(&ddt->ddt_histogram[type][class]));
// 	VERIFY(ddt_object_count(ddt, type, class, &count) == 0 && count == 0);
// 	VERIFY(zap_remove(os, DMU_POOL_DIRECTORY_OBJECT, name, tx) == 0);
// 	VERIFY(zap_remove(os, spa->spa_ddt_stat_object, name, tx) == 0);
// 	VERIFY(ddt_ops[type]->ddt_op_destroy(os, *objectp, tx) == 0);
// 	bzero(&ddt->ddt_object_stats[type][class], sizeof (ddt_object_t));

// 	*objectp = 0;
// }

static int
ddt_object_load(ddt_t *ddt, enum ddt_type type, enum ddt_class class)
{
	ddt_object_t *ddo = &ddt->ddt_object_stats[type][class];
	dmu_object_info_t doi;
	uint64_t count;
	char name[DDT_NAMELEN];
	int error;

	ddt_object_name(ddt, type, class, name);

	error = zap_lookup(ddt->ddt_os, DMU_POOL_DIRECTORY_OBJECT, name,
	    sizeof (uint64_t), 1, &ddt->ddt_object[type][class]);
	if (error != 0)
		return (error);

	error = zap_lookup(ddt->ddt_os, ddt->ddt_spa->spa_ddt_stat_object, name,
	    sizeof (uint64_t), sizeof (ddt_histogram_t) / sizeof (uint64_t),
	    &ddt->ddt_histogram[type][class]);
	if (error != 0)
		return (error);

	/*
	 * Seed the cached statistics.
	 */
	error = ddt_object_info(ddt, type, class, &doi);
	if (error)
		return (error);

	error = ddt_object_count(ddt, type, class, &count);
	if (error)
		return (error);

	ddo->ddo_count = count;
	ddo->ddo_dspace = doi.doi_physical_blocks_512 << 9;
	ddo->ddo_mspace = doi.doi_fill_count * doi.doi_data_block_size;

	return (0);
}

// static void
// ddt_object_sync(ddt_t *ddt, enum ddt_type type, enum ddt_class class,
//     dmu_tx_t *tx)
// {
// 	ddt_object_t *ddo = &ddt->ddt_object_stats[type][class];
// 	dmu_object_info_t doi;
// 	uint64_t count;
// 	char name[DDT_NAMELEN];

// 	ddt_object_name(ddt, type, class, name);

// 	VERIFY(zap_update(ddt->ddt_os, ddt->ddt_spa->spa_ddt_stat_object, name,
// 	    sizeof (uint64_t), sizeof (ddt_histogram_t) / sizeof (uint64_t),
// 	    &ddt->ddt_histogram[type][class], tx) == 0);

// 	/*
// 	 * Cache DDT statistics; this is the only time they'll change.
// 	 */
// 	VERIFY(ddt_object_info(ddt, type, class, &doi) == 0);
// 	VERIFY(ddt_object_count(ddt, type, class, &count) == 0);

// 	ddo->ddo_count = count;
// 	ddo->ddo_dspace = doi.doi_physical_blocks_512 << 9;
// 	ddo->ddo_mspace = doi.doi_fill_count * doi.doi_data_block_size;
// }

static int
ddt_object_lookup(ddt_t *ddt, enum ddt_type type, enum ddt_class class,
    ddt_entry_t *dde)
{
	if (!ddt_object_exists(ddt, type, class))
		return (SET_ERROR(ENOENT));

	return (ddt_ops[type]->ddt_op_lookup(ddt->ddt_os,
	    ddt->ddt_object[type][class], dde));
}

static void
ddt_object_prefetch(ddt_t *ddt, enum ddt_type type, enum ddt_class class,
    ddt_entry_t *dde)
{
	if (!ddt_object_exists(ddt, type, class))
		return;

	ddt_ops[type]->ddt_op_prefetch(ddt->ddt_os,
	    ddt->ddt_object[type][class], dde);
}

int
ddt_object_update(ddt_t *ddt, enum ddt_type type, enum ddt_class class,
    ddt_entry_t *dde, dmu_tx_t *tx)
{
	ASSERT(ddt_object_exists(ddt, type, class));

	return (ddt_ops[type]->ddt_op_update(ddt->ddt_os,
	    ddt->ddt_object[type][class], dde, tx));
}

// static int
// ddt_object_remove(ddt_t *ddt, enum ddt_type type, enum ddt_class class,
//     ddt_entry_t *dde, dmu_tx_t *tx)
// {
// 	ASSERT(ddt_object_exists(ddt, type, class));

// 	return (ddt_ops[type]->ddt_op_remove(ddt->ddt_os,
// 	    ddt->ddt_object[type][class], dde, tx));
// }

int
ddt_object_walk(ddt_t *ddt, enum ddt_type type, enum ddt_class class,
    uint64_t *walk, ddt_entry_t *dde)
{
	ASSERT(ddt_object_exists(ddt, type, class));

	return (ddt_ops[type]->ddt_op_walk(ddt->ddt_os,
	    ddt->ddt_object[type][class], dde, walk));
}

int
ddt_object_count(ddt_t *ddt, enum ddt_type type, enum ddt_class class,
    uint64_t *count)
{
	ASSERT(ddt_object_exists(ddt, type, class));

	return (ddt_ops[type]->ddt_op_count(ddt->ddt_os,
	    ddt->ddt_object[type][class], count));
}

int
ddt_object_info(ddt_t *ddt, enum ddt_type type, enum ddt_class class,
    dmu_object_info_t *doi)
{
	if (!ddt_object_exists(ddt, type, class))
		return (SET_ERROR(ENOENT));

	return (dmu_object_info(ddt->ddt_os, ddt->ddt_object[type][class],
	    doi));
}

boolean_t
ddt_object_exists(ddt_t *ddt, enum ddt_type type, enum ddt_class class)
{
	return (!!ddt->ddt_object[type][class]);
}

void
ddt_object_name(ddt_t *ddt, enum ddt_type type, enum ddt_class class,
    char *name)
{
	(void) snprintf(name, DDT_NAMELEN, DMU_POOL_DDT,
	    zio_checksum_table[ddt->ddt_checksum].ci_name,
	    ddt_ops[type]->ddt_op_name, ddt_class_name[class]);
}

void
ddt_bp_fill(const ddt_phys_t *ddp, blkptr_t *bp, uint64_t txg)
{
	ASSERT(txg != 0);
	/* TODO: set virtual address for bp from ddp */
	for (int d = 0; d < SPA_DVAS_PER_BP; d++)
		bp->blk_dva[d] = ddp->ddp_dva[d];
	BP_SET_BIRTH(bp, txg, ddp->ddp_phys_birth);
}

/*
 * The bp created via this function may be used for repairs and scrub, but it
 * will be missing the salt / IV required to do a full decrypting read.
 */
void
ddt_bp_create(enum zio_checksum checksum,
    const ddt_key_t *ddk, const ddt_phys_t *ddp, blkptr_t *bp)
{
	BP_ZERO(bp);

	if (ddp != NULL)
		ddt_bp_fill(ddp, bp, ddp->ddp_phys_birth);

	bp->blk_cksum = ddk->ddk_cksum;

	BP_SET_LSIZE(bp, DDK_GET_LSIZE(ddk));
	BP_SET_PSIZE(bp, DDK_GET_PSIZE(ddk));
	BP_SET_COMPRESS(bp, DDK_GET_COMPRESS(ddk));
	BP_SET_CRYPT(bp, DDK_GET_CRYPT(ddk));
	BP_SET_FILL(bp, 1);
	BP_SET_CHECKSUM(bp, checksum);
	BP_SET_TYPE(bp, DMU_OT_DEDUP);
	BP_SET_LEVEL(bp, 0);
	BP_SET_DEDUP(bp, 1);
	BP_SET_BYTEORDER(bp, ZFS_HOST_BYTEORDER);

}

void
ddt_key_fill(ddt_key_t *ddk, const blkptr_t *bp)
{
	ddk->ddk_cksum = bp->blk_cksum;
	ddk->ddk_prop = 0;

	ASSERT(BP_IS_ENCRYPTED(bp) || !BP_USES_CRYPT(bp));

	DDK_SET_LSIZE(ddk, BP_GET_LSIZE(bp));
	DDK_SET_PSIZE(ddk, BP_GET_PSIZE(bp));
	DDK_SET_COMPRESS(ddk, BP_GET_COMPRESS(bp));
	DDK_SET_CRYPT(ddk, BP_USES_CRYPT(bp));
}

void
ddt_phys_fill(ddt_phys_t *ddp, const blkptr_t *bp)
{
	ASSERT(ddp->ddp_phys_birth == 0);

	for (int d = 0; d < SPA_DVAS_PER_BP; d++)
		ddp->ddp_dva[d] = bp->blk_dva[d];
	ddp->ddp_phys_birth = BP_PHYSICAL_BIRTH(bp);
}

void
ddt_phys_clear(ddt_phys_t *ddp)
{
	bzero(ddp, sizeof (*ddp));
}

void
ddt_phys_addref(ddt_phys_t *ddp)
{
	
	ddp->ddp_refcnt++;
}

void
ddt_phys_decref(ddt_phys_t *ddp)
{
	if (ddp) {
		ASSERT(ddp->ddp_refcnt > 0);
		ddp->ddp_refcnt--;
	}
}

void
ddt_phys_free(ddt_t *ddt, ddt_key_t *ddk, ddt_phys_t *ddp, uint64_t txg)
{
	blkptr_t blk;

	ddt_bp_create(ddt->ddt_checksum, ddk, ddp, &blk);

	/*
	 * We clear the dedup bit so that zio_free() will actually free the
	 * space, rather than just decrementing the refcount in the DDT.
	 */
	BP_SET_DEDUP(&blk, 0);

	ddt_phys_clear(ddp);
	zio_free(ddt->ddt_spa, txg, &blk);
}

ddt_phys_t *
ddt_phys_select(const ddt_entry_t *dde, const blkptr_t *bp)
{
	ddt_phys_t *ddp = (ddt_phys_t *)dde->dde_phys;

	for (int p = 0; p < DDT_PHYS_TYPES; p++, ddp++) {
		if (DVA_EQUAL(BP_IDENTITY(bp), &ddp->ddp_dva[0]) &&
		    BP_PHYSICAL_BIRTH(bp) == ddp->ddp_phys_birth)
			return (ddp);
	}
	return (NULL);
}

uint64_t
ddt_phys_total_refcnt(const ddt_entry_t *dde)
{
	uint64_t refcnt = 0;

	for (int p = DDT_PHYS_SINGLE; p <= DDT_PHYS_TRIPLE; p++)
		refcnt += dde->dde_phys[p].ddp_refcnt;

	return (refcnt);
}

static void
ddt_stat_generate(ddt_t *ddt, ddt_entry_t *dde, ddt_stat_t *dds)
{
	spa_t *spa = ddt->ddt_spa;
	ddt_phys_t *ddp = dde->dde_phys;
	ddt_key_t *ddk = &dde->dde_key;
	uint64_t lsize = DDK_GET_LSIZE(ddk);
	uint64_t psize = DDK_GET_PSIZE(ddk);

	bzero(dds, sizeof (*dds));

	for (int p = 0; p < DDT_PHYS_TYPES; p++, ddp++) {
		uint64_t dsize = 0;
		uint64_t refcnt = ddp->ddp_refcnt;

		if (ddp->ddp_phys_birth == 0)
			continue;

		for (int d = 0; d < DDE_GET_NDVAS(dde); d++)
			dsize += dva_get_dsize_sync(spa, &ddp->ddp_dva[d]);

		dds->dds_blocks += 1;
		dds->dds_lsize += lsize;
		dds->dds_psize += psize;
		dds->dds_dsize += dsize;

		dds->dds_ref_blocks += refcnt;
		dds->dds_ref_lsize += lsize * refcnt;
		dds->dds_ref_psize += psize * refcnt;
		dds->dds_ref_dsize += dsize * refcnt;
	}
}

void
ddt_stat_add(ddt_stat_t *dst, const ddt_stat_t *src, uint64_t neg)
{
	const uint64_t *s = (const uint64_t *)src;
	uint64_t *d = (uint64_t *)dst;
	uint64_t *d_end = (uint64_t *)(dst + 1);

	ASSERT(neg == 0 || neg == -1ULL);	/* add or subtract */

	while (d < d_end)
		*d++ += (*s++ ^ neg) - neg;
}

static void
ddt_stat_update(ddt_t *ddt, ddt_entry_t *dde, uint64_t neg)
{
	ddt_stat_t dds;
	ddt_histogram_t *ddh;
	int bucket;

	ddt_stat_generate(ddt, dde, &dds);

	bucket = highbit64(dds.dds_ref_blocks) - 1;
	ASSERT(bucket >= 0);

	ddh = &ddt->ddt_histogram[dde->dde_type][dde->dde_class];
	// zfs_burst_dedup_dbgmsg("=====burst-dedup=====ddt: %px, bucket: %d, dde: %px, dde_type: %d, dde_class: %d, dds_ref_blocks: %llu, dds_dsize: %llu, dds_ref_dsize: %llu",ddt, bucket, dde, dde->dde_type, dde->dde_class, dds.dds_ref_blocks, dds.dds_dsize, dds.dds_ref_dsize);
	ddt_stat_add(&ddh->ddh_stat[bucket], &dds, neg);
}

void
ddt_histogram_add(ddt_histogram_t *dst, const ddt_histogram_t *src)
{
	for (int h = 0; h < 64; h++)
		ddt_stat_add(&dst->ddh_stat[h], &src->ddh_stat[h], 0);
}

void
ddt_histogram_stat(ddt_stat_t *dds, const ddt_histogram_t *ddh)
{
	bzero(dds, sizeof (*dds));

	for (int h = 0; h < 64; h++){
		ddt_stat_add(dds, &ddh->ddh_stat[h], 0);
		// zfs_burst_dedup_dbgmsg("=====burst-dedup=====dds_ref_blocks: %llu, dds_dsize: %llu, dds_ref_dsize: %llu", ddh->ddh_stat[h].dds_ref_blocks, ddh->ddh_stat[h].dds_dsize, ddh->ddh_stat[h].dds_ref_dsize);
	}
	


}

boolean_t
ddt_histogram_empty(const ddt_histogram_t *ddh)
{
	const uint64_t *s = (const uint64_t *)ddh;
	const uint64_t *s_end = (const uint64_t *)(ddh + 1);

	while (s < s_end)
		if (*s++ != 0)
			return (B_FALSE);

	return (B_TRUE);
}

void
ddt_get_dedup_object_stats(spa_t *spa, ddt_object_t *ddo_total)
{
	/* Sum the statistics we cached in ddt_object_sync(). */
	for (enum zio_checksum c = 0; c < ZIO_CHECKSUM_FUNCTIONS; c++) {
		ddt_t *ddt = spa->spa_ddt[c];
		for (enum ddt_type type = 0; type < DDT_TYPES; type++) {
			for (enum ddt_class class = 0; class < DDT_CLASSES;
			    class++) {
				ddt_object_t *ddo =
				    &ddt->ddt_object_stats[type][class];
				ddo_total->ddo_count += ddo->ddo_count;
				ddo_total->ddo_dspace += ddo->ddo_dspace;
				ddo_total->ddo_mspace += ddo->ddo_mspace;
			}
		}
	}

	/* ... and compute the averages. */
	if (ddo_total->ddo_count != 0) {
		ddo_total->ddo_dspace /= ddo_total->ddo_count;
		ddo_total->ddo_mspace /= ddo_total->ddo_count;
	}
}


void
ddt_grnerate_dedup_histogram(spa_t *spa)
{
	for (enum zio_checksum c = 0; c < ZIO_CHECKSUM_FUNCTIONS; c++) {
		ddt_t *ddt = spa->spa_ddt[c];
		ddt_entry_t *dde, *dde_next;
		bzero(&ddt->ddt_histogram, sizeof(ddt_histogram_t));
		for (dde = avl_first(&ddt->ddt_tree); dde != NULL; dde = dde_next) {
			dde_next = AVL_NEXT(&ddt->ddt_tree, dde);
			if (dde->dde_class == DDT_CLASS_DUPLICATE || dde->dde_class == DDT_CLASS_UNIQUE) {
				ddt_stat_update(ddt, dde, 0);
			}	 
 		}
	}
}

void
ddt_get_dedup_histogram(spa_t *spa, ddt_histogram_t *ddh)
{
	for (enum zio_checksum c = 0; c < ZIO_CHECKSUM_FUNCTIONS; c++) {
		ddt_t *ddt = spa->spa_ddt[c];
		for (enum ddt_type type = 0; type < DDT_TYPES; type++) {
			for (enum ddt_class class = 0; class < DDT_CLASSES;
			    class++) {
				ddt_histogram_add(ddh,
				    &ddt->ddt_histogram_cache[type][class]);
			}
		}
	}
}

void
ddt_get_dedup_stats(spa_t *spa, ddt_stat_t *dds_total)
{
	ddt_histogram_t *ddh_total;

	ddh_total = kmem_zalloc(sizeof (ddt_histogram_t), KM_SLEEP);
	ddt_get_dedup_histogram(spa, ddh_total);
	ddt_histogram_stat(dds_total, ddh_total);
	kmem_free(ddh_total, sizeof (ddt_histogram_t));
}

uint64_t
ddt_get_dedup_dspace(spa_t *spa)
{
	ddt_stat_t dds_total;

	if (spa->spa_dedup_dspace != ~0ULL)
		return (spa->spa_dedup_dspace);

	bzero(&dds_total, sizeof (ddt_stat_t));

	/* Calculate and cache the stats */
	ddt_get_dedup_stats(spa, &dds_total);
	spa->spa_dedup_dspace = dds_total.dds_ref_dsize - dds_total.dds_dsize;
	return (spa->spa_dedup_dspace);
}

uint64_t
ddt_get_pool_dedup_ratio(spa_t *spa)
{
	ddt_stat_t dds_total = { 0 };
	//ddt_grnerate_dedup_histogram(spa);
	ddt_get_dedup_stats(spa, &dds_total);
	if (dds_total.dds_dsize == 0)
		return (100);
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====dds_ref_dsize: %llu, dds_dsize: %llu",dds_total.dds_ref_dsize,dds_total.dds_dsize);
	return (dds_total.dds_ref_dsize * 100 / dds_total.dds_dsize);
}

int
ddt_ditto_copies_needed(ddt_t *ddt, ddt_entry_t *dde, ddt_phys_t *ddp_willref)
{
	spa_t *spa = ddt->ddt_spa;
	uint64_t total_refcnt = 0;
	uint64_t ditto = spa->spa_dedup_ditto;
	int total_copies = 0;
	int desired_copies = 0;
	int copies_needed = 0;

	for (int p = DDT_PHYS_SINGLE; p <= DDT_PHYS_TRIPLE; p++) {
		ddt_phys_t *ddp = &dde->dde_phys[p];
		zio_t *zio = dde->dde_lead_zio[p];
		uint64_t refcnt = ddp->ddp_refcnt;	/* committed refs */
		if (zio != NULL)
			refcnt += zio->io_parent_count;	/* pending refs */
		if (ddp == ddp_willref)
			refcnt++;			/* caller's ref */
		if (refcnt != 0) {
			total_refcnt += refcnt;
			total_copies += p;
		}
	}

	if (ditto == 0 || ditto > UINT32_MAX)
		ditto = UINT32_MAX;

	if (total_refcnt >= 1)
		desired_copies++;
	if (total_refcnt >= ditto)
		desired_copies++;
	if (total_refcnt >= ditto * ditto)
		desired_copies++;

	copies_needed = MAX(desired_copies, total_copies) - total_copies;

	/* encrypted blocks store their IV in DVA[2] */
	if (DDK_GET_CRYPT(&dde->dde_key))
		copies_needed = MIN(copies_needed, SPA_DVAS_PER_BP - 1);

	return (copies_needed);
}

int
ddt_ditto_copies_present(ddt_entry_t *dde)
{
	ddt_phys_t *ddp = &dde->dde_phys[DDT_PHYS_DITTO];
	dva_t *dva = ddp->ddp_dva;
	int copies = 0 - DVA_GET_GANG(dva);

	for (int d = 0; d < DDE_GET_NDVAS(dde); d++, dva++)
		if (DVA_IS_VALID(dva))
			copies++;

	ASSERT(copies >= 0 && copies < SPA_DVAS_PER_BP);

	return (copies);
}

size_t
ddt_compress(void *src, uchar_t *dst, size_t s_len, size_t d_len)
{
	uchar_t *version = dst++;
	int cpfunc = ZIO_COMPRESS_ZLE;
	zio_compress_info_t *ci = &zio_compress_table[cpfunc];
	size_t c_len;

	ASSERT(d_len >= s_len + 1);	/* no compression plus version byte */

	c_len = ci->ci_compress(src, dst, s_len, d_len - 1, ci->ci_level);

	if (c_len == s_len) {
		cpfunc = ZIO_COMPRESS_OFF;
		bcopy(src, dst, s_len);
	}

	*version = cpfunc;
	/* CONSTCOND */
	if (ZFS_HOST_BYTEORDER)
		*version |= DDT_COMPRESS_BYTEORDER_MASK;

	return (c_len + 1);
}

void
ddt_decompress(uchar_t *src, void *dst, size_t s_len, size_t d_len)
{
	uchar_t version = *src++;
	int cpfunc = version & DDT_COMPRESS_FUNCTION_MASK;
	zio_compress_info_t *ci = &zio_compress_table[cpfunc];

	if (ci->ci_decompress != NULL)
		(void) ci->ci_decompress(src, dst, s_len, d_len, ci->ci_level);
	else
		bcopy(src, dst, d_len);

	if (((version & DDT_COMPRESS_BYTEORDER_MASK) != 0) !=
	    (ZFS_HOST_BYTEORDER != 0))
		byteswap_uint64_array(dst, d_len);
}

ddt_t *
ddt_select(spa_t *spa, const blkptr_t *bp)
{
	return (spa->spa_ddt[BP_GET_CHECKSUM(bp)]);
}

void
ddt_enter(ddt_t *ddt)
{
	mutex_enter(&ddt->ddt_lock);
}

void
ddt_exit(ddt_t *ddt)
{
	mutex_exit(&ddt->ddt_lock);
}

void
ddt_init(void)
{
	ddt_cache = kmem_cache_create("ddt_cache",
	    sizeof (ddt_t), 0, NULL, NULL, NULL, NULL, NULL, 0);
	ddt_entry_cache = kmem_cache_create("ddt_entry_cache",
	    sizeof (ddt_entry_t), 0, NULL, NULL, NULL, NULL, NULL, 0);
}

void
ddt_fini(void)
{
	kmem_cache_destroy(ddt_entry_cache);
	kmem_cache_destroy(ddt_cache);
}

static ddt_entry_t *
ddt_alloc(const ddt_key_t *ddk)
{
	ddt_entry_t *dde;

	dde = kmem_cache_alloc(ddt_entry_cache, KM_SLEEP);
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====ddt_alloc: dde allocated dde: %px", dde);
	bzero(dde, sizeof (ddt_entry_t));
	cv_init(&dde->dde_cv, NULL, CV_DEFAULT, NULL);

	dde->dde_key = *ddk;

	return (dde);
}

static void
ddt_free(ddt_entry_t *dde)
{
	ASSERT(!dde->dde_loading);

	for (int p = 0; p < DDT_PHYS_TYPES; p++)
		ASSERT(dde->dde_lead_zio[p] == NULL);

	if (dde->dde_repair_abd != NULL)
		abd_free(dde->dde_repair_abd);

	cv_destroy(&dde->dde_cv);
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====ddt_free: freed dde: %px", dde);
	kmem_cache_free(ddt_entry_cache, dde);
}

boolean_t
ddt_exist(ddt_t *ddt, ddt_entry_t *dde)
{
	avl_index_t where;
	ASSERT(MUTEX_HELD(&ddt->ddt_lock));

	return(avl_find(&ddt->ddt_tree, dde, &where) != NULL);
}

void
ddt_remove(ddt_t *ddt, ddt_entry_t *dde)
{
	ASSERT(MUTEX_HELD(&ddt->ddt_lock));

	avl_remove(&ddt->ddt_tree, dde);
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====ddt_remove: freed dde: %px", dde);
	ddt_free(dde);
}

static void
ddt_remove_all(ddt_t *ddt)
{
	// ASSERT(MUTEX_HELD(&bstt->bstt_lock));
	ddt_entry_t *dde;
	void *cookie = NULL;
	while ((dde = avl_destroy_nodes(&ddt->ddt_tree, &cookie)) != NULL) {
		ddt_free(dde);
	}
}

ddt_entry_t *
ddt_lookup(ddt_t *ddt, const blkptr_t *bp, boolean_t add, boolean_t *found)
{
	ddt_entry_t *dde, dde_search;
	// enum ddt_type type;
	// enum ddt_class class;
	avl_index_t where;
	// int error;
	

	ASSERT(MUTEX_HELD(&ddt->ddt_lock));
	/*TODO: looking up ddt */
	ddt_key_fill(&dde_search.dde_key, bp);
	char blkbuf[BP_SPRINTF_LEN];
	snprintf_blkptr(blkbuf, sizeof(blkbuf), bp);
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====start looking up ddt... spa name: %s, bp: %s", spa_name(ddt->ddt_spa), blkbuf);
	dde = avl_find(&ddt->ddt_tree, &dde_search, &where);
	if (dde == NULL) {
		if(found != NULL){
			*found = B_FALSE;
		}
		zfs_burst_dedup_dbgmsg("=====burst-dedup=====avl_find returns NULL.");
		if (!add){
			zfs_burst_dedup_dbgmsg("=====burst-dedup=====not add, just return.");
			return (NULL);
		}
		zfs_burst_dedup_dbgmsg("=====burst-dedup=====add, so alloc a new dde and insert.");
		dde = ddt_alloc(&dde_search.dde_key);
		zfs_burst_dedup_dbgmsg("=====burst-dedup=====avl_insert: alloc and insert into ddt->ddt_tree dde: %px", dde);
		avl_insert(&ddt->ddt_tree, dde, where);
	}
	else{
		if(found != NULL){
			*found = B_TRUE;
		}
		zfs_burst_dedup_dbgmsg("=====burst-dedup=====avl_find returns dde. dde: %px", dde);
	}

	
	dde->dde_loaded = B_TRUE;
	dde->dde_loading = B_FALSE;

	zfs_burst_dedup_dbgmsg("=====burst-dedup=====End ddt_lookup. Return.");
	return (dde);
}

void
ddt_prefetch(spa_t *spa, const blkptr_t *bp)
{
	ddt_t *ddt;
	ddt_entry_t dde;

	if (!zfs_dedup_prefetch || bp == NULL || !BP_GET_DEDUP(bp))
		return;

	/*
	 * We only remove the DDT once all tables are empty and only
	 * prefetch dedup blocks when there are entries in the DDT.
	 * Thus no locking is required as the DDT can't disappear on us.
	 */
	ddt = ddt_select(spa, bp);
	ddt_key_fill(&dde.dde_key, bp);

	for (enum ddt_type type = 0; type < DDT_TYPES; type++) {
		for (enum ddt_class class = 0; class < DDT_CLASSES; class++) {
			ddt_object_prefetch(ddt, type, class, &dde);
		}
	}
}

/*
 * Opaque struct used for ddt_key comparison
 */
#define	DDT_KEY_CMP_LEN	(sizeof (ddt_key_t) / sizeof (uint16_t))

typedef struct ddt_key_cmp {
	uint16_t	u16[DDT_KEY_CMP_LEN];
} ddt_key_cmp_t;

int
ddt_entry_compare(const void *x1, const void *x2)
{
	const ddt_entry_t *dde1 = x1;
	const ddt_entry_t *dde2 = x2;
	const ddt_key_cmp_t *k1 = (const ddt_key_cmp_t *)&dde1->dde_key;
	const ddt_key_cmp_t *k2 = (const ddt_key_cmp_t *)&dde2->dde_key;
	int32_t cmp = 0;

	for (int i = 0; i < DDT_KEY_CMP_LEN; i++) {
		cmp = (int32_t)k1->u16[i] - (int32_t)k2->u16[i];
		if (likely(cmp))
			break;
	}

	return (AVL_ISIGN(cmp));
}

static ddt_t *
ddt_table_alloc(spa_t *spa, enum zio_checksum c)
{
	ddt_t *ddt;

	ddt = kmem_cache_alloc(ddt_cache, KM_SLEEP);
	bzero(ddt, sizeof (ddt_t));

	mutex_init(&ddt->ddt_lock, NULL, MUTEX_DEFAULT, NULL);
	avl_create(&ddt->ddt_tree, ddt_entry_compare,
	    sizeof (ddt_entry_t), offsetof(ddt_entry_t, dde_node));
	avl_create(&ddt->ddt_repair_tree, ddt_entry_compare,
	    sizeof (ddt_entry_t), offsetof(ddt_entry_t, dde_node));
	ddt->ddt_checksum = c;
	ddt->ddt_spa = spa;
	ddt->ddt_os = spa->spa_meta_objset;

	return (ddt);
}

static void
ddt_table_free(ddt_t *ddt)
{
	ASSERT(avl_numnodes(&ddt->ddt_tree) == 0);
	ASSERT(avl_numnodes(&ddt->ddt_repair_tree) == 0);
	avl_destroy(&ddt->ddt_tree);
	avl_destroy(&ddt->ddt_repair_tree);
	mutex_destroy(&ddt->ddt_lock);
	kmem_cache_free(ddt_cache, ddt);
}

void
ddt_create(spa_t *spa)
{
	spa->spa_dedup_checksum = ZIO_DEDUPCHECKSUM;

	for (enum zio_checksum c = 0; c < ZIO_CHECKSUM_FUNCTIONS; c++)
		spa->spa_ddt[c] = ddt_table_alloc(spa, c);
}

int
ddt_load(spa_t *spa)
{
	int error;

	ddt_create(spa);

	error = zap_lookup(spa->spa_meta_objset, DMU_POOL_DIRECTORY_OBJECT,
	    DMU_POOL_DDT_STATS, sizeof (uint64_t), 1,
	    &spa->spa_ddt_stat_object);

	if (error)
		return (error == ENOENT ? 0 : error);

	for (enum zio_checksum c = 0; c < ZIO_CHECKSUM_FUNCTIONS; c++) {
		ddt_t *ddt = spa->spa_ddt[c];
		for (enum ddt_type type = 0; type < DDT_TYPES; type++) {
			for (enum ddt_class class = 0; class < DDT_CLASSES;
			    class++) {
				error = ddt_object_load(ddt, type, class);
				if (error != 0 && error != ENOENT)
					return (error);
			}
		}

		/*
		 * Seed the cached histograms.
		 */
		bcopy(ddt->ddt_histogram, &ddt->ddt_histogram_cache,
		    sizeof (ddt->ddt_histogram));
		spa->spa_dedup_dspace = ~0ULL;
	}

	return (0);
}

void
ddt_unload(spa_t *spa)
{
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====unloading ddt tables...");
	for (enum zio_checksum c = 0; c < ZIO_CHECKSUM_FUNCTIONS; c++) {
		if (spa->spa_ddt[c]) {
			ddt_remove_all(spa->spa_ddt[c]);
			ddt_table_free(spa->spa_ddt[c]);
			spa->spa_ddt[c] = NULL;
		}
	}
}

boolean_t
ddt_class_contains(spa_t *spa, enum ddt_class max_class, const blkptr_t *bp)
{
	ddt_t *ddt;
	ddt_entry_t *dde;

	if (!BP_GET_DEDUP(bp))
		return (B_FALSE);

	if (max_class == DDT_CLASS_UNIQUE)
		return (B_TRUE);

	ddt = spa->spa_ddt[BP_GET_CHECKSUM(bp)];
	dde = kmem_cache_alloc(ddt_entry_cache, KM_SLEEP);

	ddt_key_fill(&(dde->dde_key), bp);

	for (enum ddt_type type = 0; type < DDT_TYPES; type++) {
		for (enum ddt_class class = 0; class <= max_class; class++) {
			if (ddt_object_lookup(ddt, type, class, dde) == 0) {
				kmem_cache_free(ddt_entry_cache, dde);
				return (B_TRUE);
			}
		}
	}

	kmem_cache_free(ddt_entry_cache, dde);
	return (B_FALSE);
}

ddt_entry_t *
ddt_repair_start(ddt_t *ddt, const blkptr_t *bp)
{
	ddt_key_t ddk;
	ddt_entry_t *dde;

	ddt_key_fill(&ddk, bp);

	dde = ddt_alloc(&ddk);
	zfs_burst_dedup_dbgmsg("=====burst-dedup=====ddt_repair_start: dde allocated dde: %px", dde);

	for (enum ddt_type type = 0; type < DDT_TYPES; type++) {
		for (enum ddt_class class = 0; class < DDT_CLASSES; class++) {
			/*
			 * We can only do repair if there are multiple copies
			 * of the block.  For anything in the UNIQUE class,
			 * there's definitely only one copy, so don't even try.
			 */
			if (class != DDT_CLASS_UNIQUE &&
			    ddt_object_lookup(ddt, type, class, dde) == 0)
				return (dde);
		}
	}

	bzero(dde->dde_phys, sizeof (dde->dde_phys));

	return (dde);
}

void
ddt_repair_done(ddt_t *ddt, ddt_entry_t *dde)
{
	avl_index_t where;

	ddt_enter(ddt);

	if (dde->dde_repair_abd != NULL && spa_writeable(ddt->ddt_spa) &&
	    avl_find(&ddt->ddt_repair_tree, dde, &where) == NULL){
			avl_insert(&ddt->ddt_repair_tree, dde, where);
		}
	else{
		zfs_burst_dedup_dbgmsg("=====burst-dedup=====ddt_remove: freed dde: %px", dde);
		ddt_free(dde);
	}

	ddt_exit(ddt);
}


void
ddt_histogram_set_empty(ddt_histogram_t *ddh)
{
	uint64_t *s = (uint64_t *)ddh;
	uint64_t *s_end = (uint64_t *)(ddh + 1);

	while (s < s_end)
		*s++ = 0;
}

static void
ddt_sync_table(ddt_t *ddt, dmu_tx_t *tx, uint64_t txg)
{
	spa_t *spa = ddt->ddt_spa;
	ddt_entry_t *dde, *dde_next;
	ddt_phys_t *ddp;
	ddt_key_t *ddk;
	enum ddt_type ntype = DDT_TYPE_CURRENT;
	enum ddt_class nclass;
	// void *cookie = NULL;
	
	if (avl_numnodes(&ddt->ddt_tree) == 0)
		return;

	ASSERT(spa->spa_uberblock.ub_version >= SPA_VERSION_DEDUP);


	for (enum ddt_type type = 0; type < DDT_TYPES; type++) {
		for (enum ddt_class class = 0; class < DDT_CLASSES; class++) {
			ddt_histogram_set_empty(&ddt->ddt_histogram[type][class]);
		}
	}
	// zfs_burst_dedup_dbgmsg("ddt->ddt_histogram has been clear, ddt: %px", ddt);
	for (dde = avl_first(&ddt->ddt_tree); dde != NULL; dde = dde_next) {
		dde_next = AVL_NEXT(&ddt->ddt_tree, dde);
		ddp = dde->dde_phys;
		ddk = &(dde->dde_key);
		uint64_t total_refcnt = 0;
		for (int p = 0; p < DDT_PHYS_TYPES; p++, ddp++) {
			ASSERT(dde->dde_lead_zio[p] == NULL);
			if (ddp->ddp_phys_birth == 0) {
				ASSERT(ddp->ddp_refcnt == 0);
				continue;
			}
			if (p == DDT_PHYS_DITTO) {
				/*
				 * Note, we no longer create DDT-DITTO blocks, but we
				 * don't want to leak any written by older software.
				 */
				ddt_phys_free(ddt, ddk, ddp, txg);
				continue;
			}
			if (ddp->ddp_refcnt == 0)
				ddt_phys_free(ddt, ddk, ddp, txg);
			total_refcnt += ddp->ddp_refcnt;
		}
		if (total_refcnt > 1)
			nclass = DDT_CLASS_DUPLICATE;
 		else
 			nclass = DDT_CLASS_UNIQUE;

		if (total_refcnt != 0) {
			dde->dde_type = ntype;
			dde->dde_class = nclass;
			ddt_stat_update(ddt, dde, 0);
		}	 
 	}

	 
	bcopy(ddt->ddt_histogram, &ddt->ddt_histogram_cache,
	    sizeof (ddt->ddt_histogram));
	spa->spa_dedup_dspace = ~0ULL;

}

void
ddt_sync(spa_t *spa, uint64_t txg)
{
	dsl_scan_t *scn = spa->spa_dsl_pool->dp_scan;
	dmu_tx_t *tx;
	zio_t *rio;

	ASSERT(spa_syncing_txg(spa) == txg);

	tx = dmu_tx_create_assigned(spa->spa_dsl_pool, txg);

	rio = zio_root(spa, NULL, NULL,
	    ZIO_FLAG_CANFAIL | ZIO_FLAG_SPECULATIVE | ZIO_FLAG_SELF_HEAL);

	/*
	 * This function may cause an immediate scan of ddt blocks (see
	 * the comment above dsl_scan_ddt() for details). We set the
	 * scan's root zio here so that we can wait for any scan IOs in
	 * addition to the regular ddt IOs.
	 */
	ASSERT3P(scn->scn_zio_root, ==, NULL);
	scn->scn_zio_root = rio;

	for (enum zio_checksum c = 0; c < ZIO_CHECKSUM_FUNCTIONS; c++) {
		ddt_t *ddt = spa->spa_ddt[c];
		bstt_t *bstt = spa->spa_bstt[c];
		htddt_t *hddt = spa->spa_hddt[c];
		htddt_t *tddt = spa->spa_tddt[c];
		if (ddt == NULL)
			continue;
		ddt_enter(ddt);	
		ddt_sync_table(ddt, tx, txg);
		bstt_sync_table(bstt, ddt, txg);
		htddt_sync_table(hddt, ddt);
		htddt_sync_table(tddt, ddt);
		ddt_exit(ddt);
	}

	(void) zio_wait(rio);
	scn->scn_zio_root = NULL;

	dmu_tx_commit(tx);
}

int
ddt_walk(spa_t *spa, ddt_bookmark_t *ddb, ddt_entry_t *dde)
{
	do {
		do {
			do {
				ddt_t *ddt = spa->spa_ddt[ddb->ddb_checksum];
				int error = ENOENT;
				if (ddt_object_exists(ddt, ddb->ddb_type,
				    ddb->ddb_class)) {
					error = ddt_object_walk(ddt,
					    ddb->ddb_type, ddb->ddb_class,
					    &ddb->ddb_cursor, dde);
				}
				dde->dde_type = ddb->ddb_type;
				dde->dde_class = ddb->ddb_class;
				if (error == 0)
					return (0);
				if (error != ENOENT)
					return (error);
				ddb->ddb_cursor = 0;
			} while (++ddb->ddb_checksum < ZIO_CHECKSUM_FUNCTIONS);
			ddb->ddb_checksum = 0;
		} while (++ddb->ddb_type < DDT_TYPES);
		ddb->ddb_type = 0;
	} while (++ddb->ddb_class < DDT_CLASSES);

	return (SET_ERROR(ENOENT));
}

#if defined(_KERNEL)
module_param(zfs_dedup_prefetch, int, 0644);
MODULE_PARM_DESC(zfs_dedup_prefetch, "Enable prefetching dedup-ed blks");
#endif
