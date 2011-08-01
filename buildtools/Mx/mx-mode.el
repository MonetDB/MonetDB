; The contents of this file are subject to the MonetDB Public License
; Version 1.1 (the "License"); you may not use this file except in
; compliance with the License. You may obtain a copy of the License at
; http://www.monetdb.org/Legal/MonetDBLicense
;
; Software distributed under the License is distributed on an "AS IS"
; basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
; License for the specific language governing rights and limitations
; under the License.
;
; The Original Code is the MonetDB Database System.
;
; The Initial Developer of the Original Code is CWI.
; Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
; Copyright August 2008-2011 MonetDB B.V.
; All Rights Reserved.

;;
;; Mx mode for emacs (alpha version nil)
;;

;; Author: Arjen P. de Vries <arjen@acm.org>
;; Maintainer: Arjen P. de Vries
;; Keywords: mx, monet

;; Copyright (C) 2001 Arjen P. de Vries.

;; Most of the code is bluntly copied from sml-mode.el;
;; also, it does very very little at this moment, only some
;; basic font-lock support.

;;; Commentary:
;;
;; Associate this mode to mx files in your .emacs.el as follows:
;;
;; (autoload 'mx-mode "mx-mode" "Major mode for editing Mx." t)
;; (setq auto-mode-alist
;;       (append
;;        '(("\\.mx$"	. mx-mode))
;;        auto-mode-alist))
;;

;;; Code:
(provide 'mx-mode)

(defvar mx-mode-syntax-table nil
  "Syntax table used in `mx-mode' buffers.")
(if mx-mode-syntax-table
    nil
  (setq mx-mode-syntax-table (make-syntax-table))
  (modify-syntax-entry ?@ "_ 1" mx-mode-syntax-table)
  (modify-syntax-entry ?' "_ 2" mx-mode-syntax-table)
  (modify-syntax-entry ?\n ">" mx-mode-syntax-table))

(defvar mx-mode-map ()
  "Keymap used in Mx mode.")

(if mx-mode-map
    ()
  (setq mx-mode-map (make-sparse-keymap))
  ;; (define-key mx-mode-map "\C-c\C-i" 'mx-do-something)
)

(defvar mx-font-lock-auto-on t
  "*If non-nil, turn on font-lock unconditionally for every Mx buffer.")

;; font-lock-list for mx-mode
(defvar mx-font-lock-keywords
  `(
    ("^\\(@[A-Za-z\-\\\+\\\*]+\\) \\([^@\\\n]+\\)$"
     (1 font-lock-keyword-face)
     (2 font-lock-string-face nil t))
    ("^\\(@\\w+\\)$"
     (1 font-lock-keyword-face nil t))
    ("^\\(@= .*\\)\\(.*\\)$"
     (1 font-lock-type-face)
     (2 font-lock-string-face nil t))
    ("\\(@[:#\[][^@]+@\\)"
     (1 font-lock-type-face nil t))
   )
  "Keywords map used in Mx mode"
)

(defvar mx-font-lock-all nil
  "Font-lock matchers for Mx.")

(defvar mx-font-lock-extra-keywords nil
  ;; The example is easier to read if you load this package and use C-h v
  ;; to view the documentation.
  "*List of regexps to fontify as additional Mx keywords.")

(defun mx-font-lock-setup ()
  "Set buffer-local font-lock variables and possibly turn on font-lock."
    (or mx-font-lock-all
	(setq mx-font-lock-all
	      (append
	       mx-font-lock-extra-keywords
	       mx-font-lock-keywords)))
    (make-local-variable 'font-lock-defaults)
    (setq font-lock-defaults '(mx-font-lock-all nil))
    (and mx-font-lock-auto-on (turn-on-font-lock))
)

(add-hook 'mx-mode-hook 'mx-font-lock-setup)

(defun mx-mode ()
  "Major mode for editing Mx code.
Use the hook mx-mode-hook to execute custom code when entering Mx
mode.
\\{mx-mode-map}"
  (interactive)
    ;; Now customize mx-mode to give us any behaviors specific to
    ;; Mx mode. (Hm; not much there right now...)
    (use-local-map mx-mode-map)
    (set-syntax-table mx-mode-syntax-table)
    (setq major-mode 'mx-mode
	  mode-name "Mx"
	  comment-column 32)
    (set (make-local-variable 'require-final-newline) t)
    (run-hooks 'mx-mode-hook)
)
