;;; Directory Local Variables
;;; For more information see (info "(emacs) Directory Variables")
((prog-mode
  . ((go-test-args . "-tags libsqlite3")
     (eval
      . (set
	 (make-local-variable 'flycheck-go-build-tags)
	 '("libsqlite3"))))))
