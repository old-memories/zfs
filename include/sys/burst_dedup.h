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
 * Copyright (c) 2016 by Delphix. All rights reserved.
 */

#ifndef _SYS_BURST_DEDUP_H
#define	_SYS_BURST_DEDUP_H

#include <sys/sysmacros.h>
#include <sys/types.h>
#include <sys/ddt.h>
#include <sys/zio.h>

#ifdef	__cplusplus
extern "C" {
#endif

#define MIN_BLOCK_SIZE 1
#define MAX_HTDDP_REFCNT 1
#define HTDDT_HT_RIGHTSHIFT 3

typedef struct ddt_entry ddt_entry_t;

/*
    We split a data block into three parts:
        DATA =  HEAD + MIDDLE + TAIL
    The length of HEAD and TAIL is no more than length(DATA)/2.

    If cksm(HEAD of a new block) == cksm(HEAD of BASED BLOCK) or cksm(TAIL of a new block) == cksm(TAIL of BASED BLOCK),
    We call the block SIMILAR to the based block.
    The diff between BASED BLOCK and SIMILAR BLOCK is called BURST DATA:
        BASED BLOCK + BURST DATA = SIMILAR BLOCK 
*/


/*
    classify head or tail dedup table/entry
    A HEAD entry is always associated with a HEAD block.
 */
enum htddt_type {
	HTDDT_TYPE_HEAD = 0,
	HTDDT_TYPE_TAIL = 1,
	HTDDT_TYPE_TYPES
};
/*
    a head/tail ddt entry has one key:  
        htddk_cksum(checksum of HEAD/TAIL)
        htddk_type(HEAD or TAIL)
*/
typedef struct htddt_key {
    zio_cksum_t htddk_cksum;
    enum htddt_type htddk_type;
} htddt_key_t;

/*
    a head/tail ddt entry has one value(phys): 
        htddp_dde's ptr(the dde is associated with the based block)
        htddp_refcnt(reference counter)
*/
typedef struct htddt_phys {
    boolean_t valid;
    uint8_t htddp_dde_p;
    uint64_t htddp_refcnt;
    uint64_t htddp_abd_size;
    ddt_entry_t *htddp_dde;
   
} htddt_phys_t;

/*
    A head/tail ddt entry is one AVL-tree node
*/
typedef struct htddt_entry {
    htddt_key_t htdde_key;
    htddt_phys_t htdde_phys;
    avl_node_t htdde_node;
} htddt_entry_t;

/*
    Similar to ddt
*/
typedef struct htddt {
    avl_tree_t htddt_tree;
    enum zio_checksum htddt_checksum;
    enum htddt_type htddt_type;
    spa_t *htddt_spa;
    avl_node_t htddt_node;
} htddt_t;

/*
 * Opaque struct used for htddt_key comparison
 */
#define	HTDDT_KEY_CMP_LEN	(sizeof (htddt_key_t) / sizeof (uint16_t))

typedef struct htddt_key_cmp {
	uint16_t	u16[HTDDT_KEY_CMP_LEN];
} htddt_key_cmp_t;

/*
    For simplicity, now we just store the burst data in the struct itself.
*/
typedef struct burst {
    int start_pos;
    // [start_pos, end_pos]
    int end_pos;
    size_t length;
    abd_t *burst_abd;
    uint64_t burst_abd_size;
} burst_t;

/*
    We use cksm(DATA of the similar block) as the key to reference the burst and the BASED block's dde.
    Why we do not use address or sth else:
        A block(blkptr_t in zfs) implies that the application refer the block through a valid address.
        If two SIMILAR blocks share the same cksm, we can always say that they have the same data.
        So using the cksm can find the same burst. 
*/
typedef struct bstt_key {
    zio_cksum_t bstk_cksum;
} bstt_key_t;

typedef struct bstt_phys {
    boolean_t valid;
    uint8_t bstp_dde_p;
    uint64_t bstp_refcnt;
    uint64_t bstp_abd_size;
    ddt_entry_t *bstp_dde;
    burst_t bstp_burst;
    dva_t		bstp_burst_dva[SPA_DVAS_PER_BP];
    uint64_t	bstp_phys_birth;
    uint64_t	bstp_prop;
} bstt_phys_t;

#define	BSTP_GET_LSIZE(bstp)	\
	BF64_GET_SB((bstp)->bstp_prop, 0, 16, SPA_MINBLOCKSHIFT, 1)
#define	BSTP_SET_LSIZE(bstp, x)	\
	BF64_SET_SB((bstp)->bstp_prop, 0, 16, SPA_MINBLOCKSHIFT, 1, x)

#define	BSTP_GET_PSIZE(bstp)	\
	BF64_GET_SB((bstp)->bstp_prop, 16, 16, SPA_MINBLOCKSHIFT, 1)
#define	BSTP_SET_PSIZE(bstp, x)	\
	BF64_SET_SB((bstp)->bstp_prop, 16, 16, SPA_MINBLOCKSHIFT, 1, x)

#define	BSTP_GET_COMPRESS(bstp)		BF64_GET((bstp)->bstp_prop, 32, 7)
#define	BSTP_SET_COMPRESS(bstp, x)	BF64_SET((bstp)->bstp_prop, 32, 7, x)

#define	BSTP_GET_CRYPT(bstp)		BF64_GET((bstp)->bstp_prop, 39, 1)
#define	BSTP_SET_CRYPT(bstp, x)	BF64_SET((bstp)->bstp_prop, 39, 1, x)


/*
    A burst entry is one AVL-tree node
*/
typedef struct bstt_entry {
    bstt_key_t bste_key;
    bstt_phys_t bste_phys;
    zio_t		*bste_lead_burst_io;
    avl_node_t bste_node;
} bstt_entry_t;

/*
    Similar to ddt
*/
typedef struct bstt {
    // kmutex_t bstt_lock;
    avl_tree_t bstt_tree;
    enum zio_checksum bstt_checksum;
    spa_t *bstt_spa;
    avl_node_t bstt_node;
} bstt_t;

/*
 * Opaque struct used for bstt_key comparison
 */
#define	BSTT_KEY_CMP_LEN	(sizeof (bstt_key_t) / sizeof (uint16_t))

typedef struct bstt_key_cmp {
	uint16_t	u16[BSTT_KEY_CMP_LEN];
} bstt_key_cmp_t;



/*
    head and tail dedup entry/table
*/
extern htddt_t *htddt_select(spa_t *spa, const blkptr_t *bp, enum htddt_type type);
extern void htddt_init(void);
extern void htddt_fini(void);
extern htddt_entry_t *htddt_lookup(htddt_t *htddt, htddt_key_t *htddk, boolean_t add, boolean_t *found);
extern void htddt_remove(htddt_t *htddt, htddt_entry_t *htdde);
extern void htddt_create(spa_t *spa);
extern void htddt_unload(spa_t *spa);
extern void htddt_sync_table(htddt_t *htddt, ddt_t *ddt);
extern void htddt_bstp_fill(htddt_entry_t *htdde, bstt_phys_t *bstp);
extern void htddt_phys_addref(zio_t *zio, htddt_phys_t *htddp);
extern int htddt_entry_compare(const void *x1, const void *x2);
extern uint64_t htddt_htsize(uint64_t size);
/*
    burst
*/
extern bstt_t *bstt_select(spa_t *spa, const blkptr_t *bp);
extern void bstt_init(void);
extern void bstt_fini(void);
extern bstt_entry_t *bstt_lookup(bstt_t *bstt, bstt_key_t *bstk, boolean_t add, boolean_t *found);
extern void bstt_remove(bstt_t *bstt, bstt_entry_t *bste);
extern void bstt_create(spa_t *spa);
extern void bstt_unload(spa_t *spa);
extern void bstt_sync_table(bstt_t *bstt, ddt_t *ddt, uint64_t txg);
extern int bstt_entry_compare(const void *x1, const void *x2);
extern void bstt_bstp_fill(bstt_phys_t *bstp, blkptr_t *bp);
extern void bstt_bp_fill(bstt_phys_t *bstp, blkptr_t *bp, uint64_t txg);
extern void bstt_bp_create(enum zio_checksum checksum, bstt_key_t *bstk, bstt_phys_t *bstp, blkptr_t *bp);
extern void bstt_phys_addref(zio_t *zio, bstt_phys_t *bstp);
extern void bstt_phys_free(bstt_t *bstt, bstt_key_t *bstk, bstt_phys_t *bstp, uint64_t txg);
/*
    checksum
*/
// extern void htddt_checksum_compute(htddt_key_t *htddk, enum zio_checksum htddt_checksum, abd_t *total_abd, uint64_t total_size, uint64_t ht_size);
// extern void zio_print_abd_data(zio_t *zio);

extern void bstt_create_burst(burst_t *burst, abd_t *based_data, uint64_t based_data_size, abd_t *new_data, uint64_t new_data_size);
extern void bstt_create_data(burst_t *burst, abd_t *based_data, uint64_t based_data_size, abd_t *new_data, uint64_t new_data_size);

#ifdef	__cplusplus
}
#endif

#endif	/* _SYS_BURST_DEDUP_H */
