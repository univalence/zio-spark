// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

const repoUrl = "https://github.com/univalence/zio-spark";

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'ZIO Spark',
  tagline: 'Imagine if ZIO and Spark made a baby !',
  url: 'https://univalence.github.io/',
  baseUrl: '/zio-spark/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  organizationName: 'univalence',
  projectName: 'zio-spark',

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          routeBasePath: '/',
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: `${repoUrl}/edit/master/docs/`, 
          path: '../docs',
        },
        blog: false,
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'ZIO for Spark',
        items: [
          {
            href: repoUrl,
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Documentation',
            items: [
              {
                label: 'Introduction',
                to: '/',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'ZIO',
                href: 'https://zio.dev/',
              },
              {
                label: 'Spark',
                href: 'https://spark.apache.org/docs/latest/index.html',
              },
            ],
          },
          {
            title: 'Univalence',
            items: [
              {
                label: 'Website',
                href: 'https://univalence.io/',
              },
              {
                label: 'Blog',
                href: 'https://univalence.io/blog/',
              },
              {
                label: 'Github',
                href: 'https://github.com/UNIVALENCE',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} zio-spark, Inc. Built with Docusaurus.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
};

module.exports = config;
