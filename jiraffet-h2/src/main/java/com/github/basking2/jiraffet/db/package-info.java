/**
 * Implement a simple database to support the Raft algorithm.
 *
 * There are 3 parts to this database.
 *
 * <ol>
 *     <li>{@link com.github.basking2.jiraffet.db.LogMyBatis} manages access to data specific to the raft algorithm.</li>
 *     <li>{@link com.github.basking2.jiraffet.db.KeyValueMyBatis} manages access to simple blobs.
 *         Often raft is used to agree on basic key-value pairs. This provides an embedded way to do that, but
 *         use of this is not required by any part of this code.</li>
 *     <li></li>
 * </ol>
 */
package com.github.basking2.jiraffet.db;