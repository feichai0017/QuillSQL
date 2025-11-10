// Populate the sidebar
//
// This is a script, and not included directly in the page, to control the total size of the book.
// The TOC contains an entry for each page, so if each page includes a copy of the TOC,
// the total size of the page becomes O(n**2).
class MDBookSidebarScrollbox extends HTMLElement {
    constructor() {
        super();
    }
    connectedCallback() {
        this.innerHTML = '<ol class="chapter"><li class="chapter-item expanded "><a href="introduction.html"><strong aria-hidden="true">1.</strong> Introduction</a></li><li class="chapter-item expanded "><a href="architecture.html"><strong aria-hidden="true">2.</strong> Overall Architecture</a></li><li class="chapter-item expanded "><a href="modules/overview.html"><strong aria-hidden="true">3.</strong> Module Overview</a></li><li class="chapter-item expanded affix "><li class="spacer"></li><li class="chapter-item expanded "><a href="contributing.html"><strong aria-hidden="true">4.</strong> Contributor&#39;s Guide</a></li><li class="chapter-item expanded affix "><li class="spacer"></li><li class="chapter-item expanded "><a href="modules/sql.html"><strong aria-hidden="true">5.</strong> SQL Front-End</a></li><li class="chapter-item expanded "><a href="modules/catalog.html"><strong aria-hidden="true">6.</strong> Catalog</a></li><li class="chapter-item expanded "><a href="modules/expression.html"><strong aria-hidden="true">7.</strong> Expression System</a></li><li class="chapter-item expanded "><a href="modules/plan.html"><strong aria-hidden="true">8.</strong> Query Plan</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="plan/lifecycle.html"><strong aria-hidden="true">8.1.</strong> The Lifecycle of a Query</a></li></ol></li><li class="chapter-item expanded "><a href="modules/optimizer.html"><strong aria-hidden="true">9.</strong> Query Optimizer</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="optimizer/rules.html"><strong aria-hidden="true">9.1.</strong> Rule-Based Optimization</a></li></ol></li><li class="chapter-item expanded "><a href="modules/execution.html"><strong aria-hidden="true">10.</strong> Execution Engine</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="execution/volcano.html"><strong aria-hidden="true">10.1.</strong> The Volcano Model</a></li></ol></li><li class="chapter-item expanded "><a href="modules/transaction.html"><strong aria-hidden="true">11.</strong> Transaction Manager</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="transaction/mvcc_and_2pl.html"><strong aria-hidden="true">11.1.</strong> MVCC and 2PL</a></li></ol></li><li class="chapter-item expanded "><a href="modules/storage.html"><strong aria-hidden="true">12.</strong> Storage Engine</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="storage/disk_io.html"><strong aria-hidden="true">12.1.</strong> Disk I/O</a></li><li class="chapter-item expanded "><a href="storage/page_layouts.html"><strong aria-hidden="true">12.2.</strong> Page &amp; Tuple Layout</a></li><li class="chapter-item expanded "><a href="storage/table_heap.html"><strong aria-hidden="true">12.3.</strong> Table Heap &amp; MVCC</a></li></ol></li><li class="chapter-item expanded "><a href="modules/buffer.html"><strong aria-hidden="true">13.</strong> Buffer Manager</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="buffer/page.html"><strong aria-hidden="true">13.1.</strong> Page &amp; Page Guards</a></li><li class="chapter-item expanded "><a href="buffer/buffer_pool.html"><strong aria-hidden="true">13.2.</strong> The Buffer Pool</a></li></ol></li><li class="chapter-item expanded "><a href="modules/index.html"><strong aria-hidden="true">14.</strong> Indexes</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="index/btree_index.html"><strong aria-hidden="true">14.1.</strong> B+Tree</a></li></ol></li><li class="chapter-item expanded "><a href="modules/recovery.html"><strong aria-hidden="true">15.</strong> Recovery Manager (WAL)</a></li><li><ol class="section"><li class="chapter-item expanded "><a href="recovery/aries.html"><strong aria-hidden="true">15.1.</strong> The ARIES Protocol</a></li><li class="chapter-item expanded "><a href="recovery/wal.html"><strong aria-hidden="true">15.2.</strong> Write-Ahead Logging</a></li></ol></li><li class="chapter-item expanded "><a href="modules/background.html"><strong aria-hidden="true">16.</strong> Background Services</a></li><li class="chapter-item expanded "><a href="modules/config.html"><strong aria-hidden="true">17.</strong> Configuration</a></li><li class="chapter-item expanded "><a href="modules/bin.html"><strong aria-hidden="true">18.</strong> Front-Ends (CLI / HTTP)</a></li><li class="chapter-item expanded "><a href="modules/tests.html"><strong aria-hidden="true">19.</strong> Testing &amp; Documentation</a></li></ol>';
        // Set the current, active page, and reveal it if it's hidden
        let current_page = document.location.href.toString().split("#")[0].split("?")[0];
        if (current_page.endsWith("/")) {
            current_page += "index.html";
        }
        var links = Array.prototype.slice.call(this.querySelectorAll("a"));
        var l = links.length;
        for (var i = 0; i < l; ++i) {
            var link = links[i];
            var href = link.getAttribute("href");
            if (href && !href.startsWith("#") && !/^(?:[a-z+]+:)?\/\//.test(href)) {
                link.href = path_to_root + href;
            }
            // The "index" page is supposed to alias the first chapter in the book.
            if (link.href === current_page || (i === 0 && path_to_root === "" && current_page.endsWith("/index.html"))) {
                link.classList.add("active");
                var parent = link.parentElement;
                if (parent && parent.classList.contains("chapter-item")) {
                    parent.classList.add("expanded");
                }
                while (parent) {
                    if (parent.tagName === "LI" && parent.previousElementSibling) {
                        if (parent.previousElementSibling.classList.contains("chapter-item")) {
                            parent.previousElementSibling.classList.add("expanded");
                        }
                    }
                    parent = parent.parentElement;
                }
            }
        }
        // Track and set sidebar scroll position
        this.addEventListener('click', function(e) {
            if (e.target.tagName === 'A') {
                sessionStorage.setItem('sidebar-scroll', this.scrollTop);
            }
        }, { passive: true });
        var sidebarScrollTop = sessionStorage.getItem('sidebar-scroll');
        sessionStorage.removeItem('sidebar-scroll');
        if (sidebarScrollTop) {
            // preserve sidebar scroll position when navigating via links within sidebar
            this.scrollTop = sidebarScrollTop;
        } else {
            // scroll sidebar to current active section when navigating via "next/previous chapter" buttons
            var activeSection = document.querySelector('#sidebar .active');
            if (activeSection) {
                activeSection.scrollIntoView({ block: 'center' });
            }
        }
        // Toggle buttons
        var sidebarAnchorToggles = document.querySelectorAll('#sidebar a.toggle');
        function toggleSection(ev) {
            ev.currentTarget.parentElement.classList.toggle('expanded');
        }
        Array.from(sidebarAnchorToggles).forEach(function (el) {
            el.addEventListener('click', toggleSection);
        });
    }
}
window.customElements.define("mdbook-sidebar-scrollbox", MDBookSidebarScrollbox);
