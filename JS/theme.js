/**
 * theme.js — Общий скрипт переключения тем для всего портала
 * Подключать в конце <body> на каждой странице:
 * <script src="../JS/theme.js"></script>
 *
 * Тема сохраняется в localStorage ('portal-theme') и применяется
 * сразу при загрузке любой страницы портала.
 */

(function () {
    "use strict";

    const STORAGE_KEY = 'portal-theme';
    const DEFAULT     = 'theme-current';

    /* ── Применить тему (класс на <body>) ── */
    function applyTheme(theme) {
        document.body.className = theme;
        // Синхронизируем активный пункт в дропдауне
        document.querySelectorAll('.theme-option').forEach(function (opt) {
            opt.classList.toggle('is-active', opt.dataset.theme === theme);
        });
    }

    /* ── Сохранить и применить тему ── */
    function setTheme(theme) {
        try { localStorage.setItem(STORAGE_KEY, theme); } catch (e) {}
        applyTheme(theme);
        /* Перекрасить карту при смене темы (если страница с картой) */
        if (typeof window.recolorMap === 'function') window.recolorMap();
        if (typeof window.updatePanel === 'function') window.updatePanel();
        if (typeof window.updateAbbrBadge === 'function') window.updateAbbrBadge();
    }

    /* ── Загрузить сохранённую тему ── */
    function loadTheme() {
        var saved = DEFAULT;
        try { saved = localStorage.getItem(STORAGE_KEY) || DEFAULT; } catch (e) {}
        applyTheme(saved);
    }

    /* ── Инициализация после загрузки DOM ── */
    document.addEventListener('DOMContentLoaded', function () {

        // 1. Применяем тему сразу
        loadTheme();

        // 2. Кнопка «ТЕМЫ» — открыть/закрыть дропдаун
        var toggle   = document.getElementById('themeToggle');
        var dropdown = document.getElementById('themeDropdown');

        if (!toggle || !dropdown) return;

        toggle.addEventListener('click', function (e) {
            e.stopPropagation();
            dropdown.classList.toggle('open');
        });

        // Закрыть по клику вне дропдауна
        document.addEventListener('click', function () {
            dropdown.classList.remove('open');
        });

        // Не закрывать при клике внутри дропдауна
        dropdown.addEventListener('click', function (e) {
            e.stopPropagation();
        });

        // 3. Пункты списка тем
        document.querySelectorAll('.theme-option').forEach(function (opt) {
            opt.addEventListener('click', function () {
                setTheme(this.dataset.theme);
                dropdown.classList.remove('open');
            });
        });

        // 4. Год в футере
        var yearEl = document.getElementById('year');
        if (yearEl) yearEl.textContent = new Date().getFullYear();
    });

})();
