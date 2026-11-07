package wikihadoop;

/**
 * What became of one {@code pagelinks} row (plan section 13). Every row ends in exactly one, so the
 * fates of the Edges and Adjacency jobs together sum to the table's rows. Checked in this order:
 * where the link comes from (namespace), where it goes, then where it comes from (page), then
 * whether it is a self-link or a duplicate.
 */
public enum LinkFate {
  /** An edge, at least one of whose rows names the target article directly. */
  EDGE_DIRECT,
  /** An edge whose rows all reach the target through a redirect. */
  EDGE_VIA,
  /** Another row for an edge already counted. */
  DUPLICATE,
  /** Resolves to the page it comes from. */
  SELF,
  FROM_OTHER_NAMESPACE,
  FROM_REDIRECT,
  /** From a page id that the page table does not hold. */
  FROM_MISSING,
  /** A namespace 0 title with no page. */
  RED_LINK,
  TARGET_OTHER_NAMESPACE,
  /** A link target id that the linktarget table does not hold. */
  TARGET_ID_MISSING,
  /** To a redirect whose target is itself a redirect: one hop only. */
  DOUBLE_REDIRECT,
  REDIRECT_OUT_OF_NS0,
  REDIRECT_INTERWIKI,
  /** To a redirect whose target title has no page. */
  REDIRECT_TO_MISSING,
  /** To a page marked as a redirect that the redirect table has no row for. */
  REDIRECT_WITHOUT_ROW
}
