/*= -*- c-basic-offset: 4; indent-tabs-mode: nil; -*-
 *
 * librsync -- the library for network deltas
 * $Id: buf.c,v 1.22 2003/12/16 00:10:55 abo Exp $
 * 
 * Copyright (C) 2000, 2001 by Martin Pool <mbp@samba.org>
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */

                              /*
                               | Pick a window, Jimmy, you're leaving.
                               |   -- Martin Schwenke, regularly
                               */


/*
 * buf.c -- Buffers that map between stdio file streams and librsync
 * streams.  As the stream consumes input and produces output, it is
 * refilled from appropriate input and output FILEs.  A dynamically
 * allocated buffer of configurable size is used as an intermediary.
 *
 * TODO: Perhaps be more efficient by filling the buffer on every call
 * even if not yet completely empty.  Check that it's really our
 * buffer, and shuffle remaining data down to the front.
 *
 * TODO: Perhaps expose a routine for shuffling the buffers.
 */


#include <config.h>
#include <sys/types.h>

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>

#include "librsync.h"
#include "trace.h"
#include "buf.h"
#include "util.h"

/* use fseeko instead of fseek for long file support if we have it */
#ifdef HAVE_FSEEKO
#define fseek fseeko
#endif

/**
 * File IO buffer sizes.
 */
LIBRSYNC_API
size_t rs_inbuflen = 16000, rs_outbuflen = 16000;


struct rs_filebuf {
        MFILE *f;
        char            *buf;
        size_t          buf_len;
};


rs_filebuf_t *rs_filebuf_new(MFILE *f, size_t buf_len) 
{
    rs_filebuf_t *pf = rs_alloc_struct(rs_filebuf_t);

    pf->buf = rs_alloc(buf_len, "file buffer");
    pf->buf_len = buf_len;
    pf->f = f;

    return pf;
}


void rs_filebuf_free(rs_filebuf_t *fb) 
{
	free(fb->buf);
        rs_bzero(fb, sizeof *fb);
        free(fb);
}


/*
 * If the stream has no more data available, read some from F into
 * BUF, and let the stream use that.  On return, SEEN_EOF is true if
 * the end of file has passed into the stream.
 */
rs_result rs_infilebuf_fill(rs_job_t *job, rs_buffers_t *buf,
                            void *opaque)
{
    size_t                  len;
    rs_filebuf_t            *fb = (rs_filebuf_t *) opaque;
    MFILE                    *f = fb->f;
        
    /* This is only allowed if either the buf has no input buffer
     * yet, or that buffer could possibly be BUF. */
    if (buf->next_in != NULL) {
        assert(buf->avail_in <= fb->buf_len);
        assert(buf->next_in >= fb->buf);
        assert(buf->next_in <= fb->buf + fb->buf_len);
    } else {
        assert(buf->avail_in == 0);
    }

    if (buf->eof_in) {
        rs_trace("seen end of file on input");
        buf->eof_in = 1;
        return RS_DONE;
    }

    if (buf->avail_in)
        /* Still some data remaining.  Perhaps we should read
           anyhow? */
        return RS_DONE;
        
    if ( f->fptr >= f->len ) {
        buf->eof_in = 1;
        return RS_DONE;
    }
    len = fb->buf_len;
    if ((f->len - f->fptr) < len) {
       len = (f->len)-(f->fptr);
    }
    memcpy(fb->buf, f->src+f->fptr, len);
    f->fptr += len;

    buf->avail_in = len;
    buf->next_in = fb->buf;

    return RS_DONE;
}


/*
 * The buf is already using BUF for an output buffer, and probably
 * contains some buffered output now.  Write this out to F, and reset
 * the buffer cursor.
 */
rs_result rs_outfilebuf_drain(rs_job_t *job, rs_buffers_t *buf, void *opaque)
{
    size_t present;
    rs_filebuf_t *fb = (rs_filebuf_t *) opaque;
    MFILE *f = fb->f;

    /* This is only allowed if either the buf has no output buffer
     * yet, or that buffer could possibly be BUF. */
    if (buf->next_out == NULL) {
        assert(buf->avail_out == 0);
                
        buf->next_out = fb->buf;
        buf->avail_out = fb->buf_len;
                
        return RS_DONE;
    }
        
    assert(buf->avail_out <= fb->buf_len);
    assert(buf->next_out >= fb->buf);
    assert(buf->next_out <= fb->buf + fb->buf_len);

    present = buf->next_out - fb->buf;
    if (present > 0) {
        int result;
                
        assert(present > 0);

        result = present < (f->len - f->fptr) ? present : (f->len - f->fptr);
        memcpy(f->src + f->fptr, fb->buf, result);
        f->fptr += result;
        if (present != result) {
            return RS_IO_ERROR;
        }

        buf->next_out = fb->buf;
        buf->avail_out = fb->buf_len;
    }

    return RS_DONE;
}


/**
 * Default copy implementation that retrieves a part of a stdio file.
 */
rs_result rs_file_copy_cb(void *arg, rs_long_t pos, size_t *len, void **buf)
{
    int        got;
    MFILE       *f = (MFILE *) arg;

    f->fptr = pos;

    got = *len < (f->len - f->fptr) ? *len : (f->len - f->fptr);
    memcpy(*buf, f->src + f->fptr, got);
    f->fptr += got;

    if (got == -1) {
        rs_error("%s", strerror(errno));
        return RS_IO_ERROR;
    } else if (got == 0) {
        rs_error("unexpected eof on fd%d", 0);
        return RS_INPUT_ENDED;
    } else {
        *len = got;
        return RS_DONE;
    }
}
